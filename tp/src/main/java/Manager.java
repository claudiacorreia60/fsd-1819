import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;


public class Manager {

    // TODO: Fazer timeout

    private Serializer s;
    private ManagedMessagingService ms;
    private ExecutorService es;
    private int transactionId;
    private Map<Integer, Map<Address, Boolean>> participants; // <transaction ID, <participant, reply>>
    private Log log;


    public Manager(ManagedMessagingService ms, ExecutorService es) throws ExecutionException, InterruptedException {
        this.s = Serializer.builder()
                .withTypes(
                        Msg.class,
                        AbstractMap.SimpleEntry.class)
                .build();
        this.ms = ms;
        this.es = es;
        this.transactionId = 0;
        this.participants = new HashMap<>();
        this.log = new Log("manager");


        /* -----  FORWARDER -> MANAGER ----- */

        // Receive forwarder begin transaction
        this.ms.registerHandler("Forwarder-begin", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Handle forwarder begin
            List<Address> data = ((List<String>) msg.getData()).stream().map(s -> Address.from(s)).collect(Collectors.toList());
            Set<Address> participants = new HashSet<>(data);
            return contextHandler(participants, o);
        });


        /* -----  SERVER -> MANAGER ----- */

        // Receive prepare reply from server
        this.ms.registerHandler("Server-prepared", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Handle forwarder begin
            preparedHandler((Integer) msg.getData(), o);
        }, this.es);

        // Receive abort reply from server
        this.ms.registerHandler("Server-abort", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Handle forwarder begin
            abort((Integer) msg.getData());
        }, this.es);


        this.ms.start().get();
    }


    public CompletableFuture<byte[]> contextHandler(Set<Address> participantServers, Address forwarderAddr) {
        Map<Address, Boolean> participants = new HashMap<>();

        // Create participants list
        for(Address a : participantServers){
            participants.put(a, false);
        }

        // Store participant servers in participants Map
        this.participants.put(this.transactionId, participants);

        // Store Initialized transaction in log
        LogEntry le = new LogEntry("Initialized", this.transactionId, participantServers.stream()
                .map(a -> a.toString())
                .collect(Collectors.toList()));
        this.log.append(le);

        //Send transaction id to forwarder
        Msg msg = new Msg(this.transactionId);
        this.ms.sendAsync(forwarderAddr, "Manager-context", this.s.encode(msg));

        this.transactionId++;
        CompletableFuture<byte[]> cf = new CompletableFuture<>();
        try {
            cf.complete(this.s.encode(new Msg(this.transactionId-1)));
        } catch (Exception e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    public void commit(int transactionId){
        // Send commit message to participant servers
        for(Address a : this.participants.get(transactionId).keySet()){
            Msg msg = new Msg(transactionId);
            ms.sendAsync(a, "Manager-commit", this.s.encode(msg));
        }

        // Store committed transaction in log
        LogEntry le = new LogEntry("Committed", transactionId);
        this.log.append(le);
    }

    public void preparedHandler(int transactionId, Address serverAddr) {
        Map<Address, Boolean> serverResponses = this.participants.get(transactionId);

        // Change server response
        serverResponses.put(serverAddr, true);
        this.participants.put(transactionId, serverResponses);

        // Store prepared server in log
        LogEntry le = new LogEntry("Prepared", transactionId, serverAddr.toString());
        this.log.append(le);

        // Check if all servers are prepared
        boolean ready = true;
        for(Map.Entry<Address, Boolean> e : serverResponses.entrySet()){
            if(!e.getValue()){
                ready = false;
            }
        }

        // All servers are prepared
        if(ready) {
            commit(transactionId);
        }
    }

    public void abort(int transactionId){
        // Send abort message to participant servers
        for(Address a : this.participants.get(transactionId).keySet()){
            Msg msg = new Msg(transactionId);
            ms.sendAsync(a, "Manager-abort", this.s.encode(msg));
        }

        // Store aborted transaction in log
        LogEntry le = new LogEntry("Aborted", transactionId);
        this.log.append(le);
    }


    public void validateLog(Map<Integer, String> transactions){
        for(Map.Entry<Integer, String> e : transactions.entrySet()){
            if(e.getValue().equals("Initialized")){
                for(Address a : this.participants.get(e.getKey()).keySet()){
                    Msg msg = new Msg(e.getKey());
                    this.ms.sendAsync(a, "Manager-prepared",this.s.encode(msg));
                }
            }
        }
    }

    public void loadLog(){
        SegmentedJournalReader<Object> r = this.log.read();
        Map<Integer, String> transactions = new HashMap<>();

        while(r.hasNext()){
            LogEntry le = (LogEntry) r.next().entry();

            if(le.entryType.equals("Initialized")){
                Map<Address, Boolean> participantServers = new HashMap<>();

                // Create participants list
                for(String a : le.participants){
                    participantServers.put(Address.from(a), false);
                }

                // Store participant servers in participants Map
                this.participants.put(le.transactionId, participantServers);
            }

            // Update transactions Map
            transactions.put(le.transactionId, le.entryType);
        }

        validateLog(transactions);
    }
}