import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class Forwarder {
    private Serializer s;
    private ManagedMessagingService ms;
    private ExecutorService es;
    private Address managerAddr;
    private int getRequestId;
    private Map<Integer, Address> servers;
    private Map<Integer, PutRequest> putRequests;
    private Map<Integer, GetRequest> getRequests;


    public Forwarder(ManagedMessagingService ms, ExecutorService es, Address managerAddr) {
        this.s = Serializer.builder()
                    .withTypes(
                        Msg.class)
                    .build();
        this.ms = ms;
        this.es = es;
        this.managerAddr = managerAddr;
        this.getRequestId = 0;
        this.servers = new HashMap<>();

        this.servers.put(0, Address.from("localhost:1231"));
        this.servers.put(1, Address.from("localhost:1232"));
        this.servers.put(2, Address.from("localhost:1233"));
        this.servers.put(3, Address.from("localhost:1234"));

        this.putRequests = new HashMap<>();
        this.getRequests = new HashMap<>();


        /* -----  CLIENT -> FORWARDER ----- */

        // Receive client get request
        ms.registerHandler("Client-get", (o, m) -> {
            Msg msg = s.decode(m);

            // Handle client get request
            getHandler((Collection<Long>) msg.getData(), o);
        }, this.es);

        // Receive client put request
        ms.registerHandler("Client-put", (o, m) -> {
            Msg msg = s.decode(m);

            // Handle client put request
            try {
                putHandler((Map<Long, byte[]>) msg.getData(), o);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }, this.es);


        /* -----  SERVER -> FORWARDER ----- */

        // Receive server 2PC affirmative reply
        ms.registerHandler("Server-true", (o, m) -> {
            Msg msg = this.s.decode(m);

            /* TODO: handlePutResponse
                - ir atualizando o map putRequests
                - sempre que faz uma atualização vê se já responderam todos os
                  participantes e, se sim, envia a resposta ao cliente
                NOTA: Ver que a classe Stub (que usa sendAndReceive)
             */

            // Update putRequests Map and reply to client
            handlePutResponse((Integer) msg.getData(), true, o);
        }, this.es);

        // Receive server 2PC negative reply
        ms.registerHandler("Server-false", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Update transactions Map and reply to client
            handlePutResponse((Integer) msg.getData(), true, o);
        }, this.es);

        // Receive get response from server
        ms.registerHandler("Server-get", (o, m) -> {
            Msg msg = this.s.decode(m);

            /* TODO: handleGetResponse
                - ir atualizando o map getRequests
                - sempre que faz uma atualização vê se já responderam todos os
                  participantes e, se sim, envia a resposta ao cliente
                NOTA: Ver que a classe Stub (que usa sendAndReceive)
             */

            // Update getRequests Map and reply to client
            handleGetResponse((Map<Long, byte[]>) msg.getData(), o);
        }, this.es);
    }

    private void getHandler(Collection<Long> requestedKeys, Address client) {
        // Get participant servers
        Map<Address, Collection<Long>> participantServers = new HashMap<>();

        for(Long key : requestedKeys){
            // Calculate server ID
            int serverId = (int) (key % this.servers.size());
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

        // Inform participant servers of the get request
        for(Map.Entry<Address, Collection<Long>> participant : participantServers.entrySet()){
            Map<Integer, Collection<Long>> data = new HashMap<>();
            data.put(this.getRequestId, participant.getValue());
            Msg msg = new Msg(participant.getValue());
            this.ms.sendAsync(participant.getKey(), "Forwarder-get", this.s.encode(msg));
        }

        // Update getRequests Map
        Map<Address, Map<Long, byte[]>> participants = new HashMap<>();
        for(Address a : participantServers.keySet()){
            participants.put(a, null);
        }
        GetRequest gr = new GetRequest(this.getRequestId, participants, client);
        getRequests.put(this.getRequestId, gr);

        this.getRequestId ++;
    }

    private int beginTransaction(Map<Address, Map<Long, byte[]>> participantServers) throws ExecutionException, InterruptedException {
        // Ask Manager to begin a new transaction
        Msg msg = new Msg(participantServers.keySet());
        CompletableFuture<byte[]> cf = this.ms.sendAndReceive(this.managerAddr, "Forwarder-begin", this.s.encode(msg));
        int transactionId = this.s.decode(cf.get());

        // Inform participant servers of the transaction
        for(Map.Entry<Address, Map<Long, byte[]>> participant : participantServers.entrySet()){
            Map<Integer, Map<Long, byte[]>> data = new HashMap<>();
            data.put(transactionId, participant.getValue());
            msg = new Msg(participant.getValue());
            this.ms.sendAsync(participant.getKey(), "Forwarder-put", this.s.encode(msg));
        }

        return transactionId;
    }

    private void putHandler(Map<Long, byte[]> requestedPairs, Address client) throws ExecutionException, InterruptedException {
        // Get participant servers
        Map<Address, Map<Long, byte[]>> participantServers = new HashMap<>();

        for(Map.Entry<Long, byte[]> pair : requestedPairs.entrySet()){
            // Calculate server ID
            int serverId = (int) (pair.getKey() % this.servers.size());
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

        // Begin transaction
        int putId = beginTransaction(participantServers);

        // Update putRequests Map
        Map<Address, Integer> participants = new HashMap<>();
        for(Address a : participantServers.keySet()){
            participants.put(a, 0);
        }
        PutRequest pr = new PutRequest(putId, participants, client);
        putRequests.put(putId, pr);
    }
}
