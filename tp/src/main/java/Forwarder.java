import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class Forwarder{
    private Log log;
    private Serializer s;
    private ManagedMessagingService ms;
    private ExecutorService es;
    private Address managerAddr;
    private int getRequestId;
    private Map<Long, Address> servers;
    private Map<Integer, PutRequest> putRequests;
    private Map<Integer, GetRequest> getRequests;


    public Forwarder(ManagedMessagingService ms, ExecutorService es, Address managerAddr, String id) {
        this.log = new Log("forwarder-" + id);
        this.log.open(0);
        this.s = Serializer.builder()
                    .withTypes(
                        Msg.class,
                        AbstractMap.SimpleEntry.class,
                        PutRequest.class,
                        GetRequest.class)
                    .build();
        this.ms = ms;
        this.es = es;
        this.managerAddr = managerAddr;
        this.getRequestId = 0;
        this.servers = new HashMap<>();

        this.servers.put((long) 0, Address.from("localhost:1231"));
        this.servers.put((long) 1, Address.from("localhost:1232"));
        this.servers.put((long) 2, Address.from("localhost:1233"));
        this.servers.put((long) 3, Address.from("localhost:1234"));

        this.putRequests = new HashMap<>();
        this.getRequests = new HashMap<>();

        this.loadLog();

        /* -----  CLIENT -> FORWARDER ----- */

        // Receive client get request
        ms.registerHandler("Client-get", (o, m) -> {
            Msg msg = s.decode(m);

            // Handle client get request
            CompletableFuture<byte[]> cf = getHandler((Collection<Long>) msg.getData(), o);

            return cf;
        });

        // Receive client put request
        ms.registerHandler("Client-put", (o, m) -> {
            Msg msg = s.decode(m);

            // Handle client put request
            CompletableFuture<byte[]> cf = null;
            try {
                cf = putHandler((Map<Long, byte[]>) msg.getData(), o);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return cf;
        });


        /* -----  SERVER -> FORWARDER ----- */

        // Receive get response from server
        ms.registerHandler("Server-get", (o, m) -> {
            Msg msg = this.s.decode(m);
            AbstractMap.SimpleEntry<Integer, Map<Long, byte[]>> data = (AbstractMap.SimpleEntry<Integer, Map<Long, byte[]>>) msg.getData();

            // Update getRequests Map and reply to client
            handleGetResponse(data.getKey(), data.getValue(), o);
        }, this.es);

        // Receive server 2PC affirmative reply
        ms.registerHandler("Server-true", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Update putRequests Map and reply to client
            handlePutResponse((Integer) msg.getData(), true, o);
        }, this.es);

        // Receive server 2PC negative reply
        ms.registerHandler("Server-false", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Update transactions Map and reply to client
            handlePutResponse((Integer) msg.getData(), false, o);
        }, this.es);

        /* -----  MANAGER -> FORWARDER ----- */

        // Receive manager response to the isTransactionReady question
        ms.registerHandler("Manager-transactionIsReady", (o, m) -> {
            Msg msg = this.s.decode(m);
            Map.Entry<Integer, Boolean> answer = (AbstractMap.SimpleEntry<Integer, Boolean>) msg.getData();
            PutRequest pr = this.putRequests.get(answer.getKey());

            // Reply to client
            this.ms.sendAsync(Address.from(pr.getClientAddr()), "Put-completed", this.s.encode(msg));

            // Update putRequests Map and save it to log
            Map<Address, Integer> participants = pr.getParticipants();
            pr.getParticipants().replaceAll((k, v) -> v = 1);
            pr.setParticipants(participants);
            pr.setCompleted(true);
            putRequests.put(answer.getKey(), pr);
            LogEntry le = new LogEntry(pr);
            this.log.append(le);

        }, this.es);
    }

    private CompletableFuture<byte[]> getHandler(Collection<Long> requestedKeys, Address client) {
        // Get participant servers
        Map<Address, Collection<Long>> participantServers = new HashMap<>();

        for(Long key : requestedKeys){
            // Calculate server ID
            long serverId = (key % this.servers.size());
            Address serverAddr = this.servers.get(serverId);

            // Update participantServers Map
            Collection<Long> participantKeys;

            if(participantServers.containsKey(serverAddr)){
                participantKeys= participantServers.get(serverAddr);
            } else{
                participantKeys = new ArrayList<>();
            }

            participantKeys.add(key);
            participantServers.put(serverAddr, participantKeys);
        }

        // Update getRequests Map
        Map<Address, Map<Long, byte[]>> participants = new HashMap<>();
        for(Address a : participantServers.keySet()){
            participants.put(a, null);
        }

        // Add GetRequest to the map and save it to the log
        GetRequest gr = new GetRequest(this.getRequestId, client.toString(), participants, participantServers);
        getRequests.put(this.getRequestId, gr);
        LogEntry le = new LogEntry(gr);
        this.log.append(le);

        // Inform participant servers of the get request
        for(Map.Entry<Address, Collection<Long>> participant : participantServers.entrySet()){
            AbstractMap.SimpleEntry<Integer, Collection<Long>> data = new AbstractMap.SimpleEntry<>(this.getRequestId, participant.getValue());
            Msg msg = new Msg(data);
            this.ms.sendAsync(participant.getKey(), "Forwarder-get", this.s.encode(msg));
        }

        Msg msg = new Msg(this.getRequestId);
        CompletableFuture<byte[]> cf = CompletableFuture.completedFuture(this.s.encode(msg));

        // Increment get requests ID
        this.getRequestId ++;

        return cf;
    }

    private int beginTransaction(Map<Address, Map<Long, byte[]>> participantServers) throws ExecutionException, InterruptedException {
        // Ask Manager to begin a new transaction
        Msg msg = new Msg(participantServers.keySet().stream()
                .map(a -> a.toString())
                .collect(Collectors.toList()));
        CompletableFuture<byte[]> cf = this.ms.sendAndReceive(this.managerAddr, "Forwarder-begin", this.s.encode(msg));
        int transactionId = (Integer) ((Msg) this.s.decode(cf.get())).getData();

        // Inform participant servers of the transaction
        for(Map.Entry<Address, Map<Long, byte[]>> participant : participantServers.entrySet()){
            AbstractMap.SimpleEntry<Integer, Map<Long, byte[]>> data = new AbstractMap.SimpleEntry<>(transactionId, participant.getValue());
            msg = new Msg(data);
            this.ms.sendAsync(participant.getKey(), "Forwarder-put", this.s.encode(msg));
        }

        return transactionId;
    }

    private CompletableFuture<byte[]> putHandler(Map<Long, byte[]> requestedPairs, Address client) throws ExecutionException, InterruptedException {
        // Get participant servers
        Map<Address, Map<Long, byte[]>> participantServers = new HashMap<>();

        for(Map.Entry<Long, byte[]> pair : requestedPairs.entrySet()){
            // Calculate server ID
            long serverId = (pair.getKey() % this.servers.size());
            Address serverAddr = this.servers.get(serverId);

            // Update participantServers Map
            Map<Long, byte[]> participantPairs;

            if(participantServers.containsKey(serverAddr)){
                participantPairs = participantServers.get(serverAddr);
            } else{
                participantPairs = new HashMap<>();
            }

            participantPairs.put(pair.getKey(), pair.getValue());
            participantServers.put(serverAddr, participantPairs);
        }

        // Update putRequests Map
        Map<Address, Integer> participants = new HashMap<>();
        for(Address a : participantServers.keySet()){
            participants.put(a, 0);
        }

        // Begin transaction
        int putId = beginTransaction(participantServers);

        // Add PutRequest to the map and save it to the log
        PutRequest pr = new PutRequest(putId, client.toString(), participants);
        putRequests.put(putId, pr);
        LogEntry le = new LogEntry(pr);
        this.log.append(le);

        Msg msg = new Msg(putId);
        return CompletableFuture.completedFuture(this.s.encode(msg));
    }

    private void handleGetResponse(Integer getId, Map<Long, byte[]> serverReply, Address client) {
        GetRequest gr = getRequests.get(getId);
        Map<Address, Map<Long, byte[]>> participants = gr.getParticipants();

        // Server replied affirmatively to the put request
        participants.put(client, serverReply);

        // Count how many servers already replied to the request
        Map<Long, byte[]> replies = new HashMap<>();
        int count = 0;
        for(Address a : participants.keySet()){
            // Store responses in replies list
            Map<Long, byte[]> reply = participants.get(a);
            if(reply != null) {
                replies.putAll(reply);
                count ++;
            }
        }

        // Check if all servers replied to the request
        if(count == participants.size()){

            // Reply to the client
            Map.Entry<Integer, Map<Long, byte[]>> answer = new AbstractMap.SimpleEntry<>(gr.getTransactionId(), replies);
            Msg msg = new Msg(answer);
            this.ms.sendAsync(Address.from(gr.getClientAddr()), "Get-completed", this.s.encode(msg));
        }

        // Update putRequests Map and save it to log
        gr.setParticipants(participants);
        gr.setCompleted(true);
        getRequests.put(getId, gr);
        LogEntry le = new LogEntry(gr);
        this.log.append(le);
    }

    private void handlePutResponse(Integer putId, boolean serverReply, Address client) {
        PutRequest pr = putRequests.get(putId);
        Map<Address, Integer> participants = pr.getParticipants();

        // Server replied affirmatively to the put request
        if(serverReply){
            participants.put(client, 1);

        // Server replied negatively to the put request
        } else{
            participants.put(client, 2);
        }

        // Count how many servers already replied to the request
        List<Boolean> replies = new ArrayList<>();
        for(Address a : participants.keySet()){
            int reply = participants.get(a);
            // Store responses in replies list
            if(reply == 1) {
                replies.add(true);
            } else if(reply == 2) {
                replies.add(false);
            }
        }

        // Check if all servers replied to the request
        if(replies.size() == participants.size()){
            boolean reply = true;

            // Calculate conjunction of all servers replies
            for(boolean r : replies){
                reply = reply && r;
            }

            // Reply to the client
            Map.Entry<Integer, Boolean> answer = new AbstractMap.SimpleEntry<>(pr.getTransactionId(), reply);
            Msg msg = new Msg(answer);
            this.ms.sendAsync(Address.from(pr.getClientAddr()), "Put-completed", this.s.encode(msg));
        }

        // Update putRequests Map and save it to log
        pr.setParticipants(participants);
        pr.setCompleted(true);
        putRequests.put(putId, pr);
        LogEntry le = new LogEntry(pr);
        this.log.append(le);
    }

    // Performs certain actions based on the state of a given transaction
    private void validateLog(HashMap<Integer, PutRequest> prs, HashMap<Integer, GetRequest> grs) {

        // Handle PutRequests that are not completed
        for(PutRequest pr : prs.values()) {
            if (! pr.isCompleted()) {
                Msg msg = new Msg(pr.getTransactionId());
                this.ms.sendAsync(this.managerAddr, "Forwarder-isTransactionReady", this.s.encode(msg));
            }
        }

        // Handle GetRequests that are not completed
        for(GetRequest gr : grs.values()) {

            if (! gr.isCompleted()) {
                Map<Address, Map<Long, byte[]>> participants = gr.getParticipants();

                // Count how many servers already replied to the request
                List<Address> replies = new ArrayList<>();
                for (Address a : participants.keySet()) {

                    // Store responses in replies list
                    Map<Long, byte[]> reply = participants.get(a);
                    if (reply == null) {
                        replies.add(a);
                    }
                }

                // Ask clients for the missing pairs
                for (Address a : replies) {
                    Collection<Long> requestedKeys = gr.getRequestedKeys().get(a);
                    AbstractMap.SimpleEntry<Integer, Collection<Long>> data = new AbstractMap.SimpleEntry<>(gr.getTransactionId(), requestedKeys);
                    Msg msg = new Msg(data);
                    this.ms.sendAsync(a, "Forwarder-get", this.s.encode(msg));
                }
            }
        }
    }

    // Parses the log
    private void loadLog() {
        SegmentedJournalReader<Object> r = this.log.read();

        HashMap<Integer, PutRequest> putTransactions = new HashMap<>();
        HashMap<Integer, GetRequest> getTransactions = new HashMap<>();

        while(r.hasNext()) {
            LogEntry le = (LogEntry) r.next().entry();

            if (le.pr != null) {
                putTransactions.put(le.pr.getTransactionId(), le.pr);
            }

            if (le.gr != null) {
                getTransactions.put(le.gr.getTransactionId(), le.gr);
            }

        }

        this.validateLog(putTransactions, getTransactions);
    }
}
