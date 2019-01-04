import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ClientStub {
    private final ManagedMessagingService ms;
    private final Address forwarderAddr;
    private final Serializer s;
    private ExecutorService es;
    private Map<Integer, CompletableFuture<Boolean>> putRequests;
    private Map<Integer, CompletableFuture<Map<Long, byte[]>>> getRequests;


    public ClientStub(String myAddr, String forwarderAddr) throws Exception {
        this.forwarderAddr = Address.from(forwarderAddr);
        ms = NettyMessagingService.builder().withAddress(Address.from(myAddr)).build();
        s = Serializer.builder()
                .withTypes(
                    Msg.class,
                    AbstractMap.SimpleEntry.class)
                .build();
        this.es = Executors.newSingleThreadExecutor();
        this.putRequests = new HashMap<>();
        this.getRequests = new HashMap<>();

        /* -----  FORWARDER -> STUB ----- */

        // Whenever a transaction is completed response is received
        this.ms.registerHandler("Put-completed", (o,m) -> {
            Msg msg = this.s.decode(m);
            Map.Entry<Integer, Boolean> result = (AbstractMap.SimpleEntry<Integer, Boolean>) msg.getData();

            // Complete de CompletableFuture related to the transactionId
            if (this.putRequests.containsKey(result.getKey())) {
                this.putRequests.get(result.getKey()).complete(result.getValue());
                this.putRequests.remove(result.getKey());
            }

        }, this.es);

        // Whenever a transaction is completed response is received
        this.ms.registerHandler("Get-completed", (o,m) -> {
            Msg msg = this.s.decode(m);
            Map.Entry<Integer, Map<Long, byte[]>> result = (AbstractMap.SimpleEntry<Integer, Map<Long,byte[]>>) msg.getData();

            // Complete de CompletableFuture related to the transactionId
            if (this.getRequests.containsKey(result.getKey())) {
                this.getRequests.get(result.getKey()).complete(result.getValue());
                this.getRequests.remove(result.getKey());
            }

        }, this.es);

        ms.start().get();
    }

    CompletableFuture<Boolean> put (Map<Long, byte[]> values) throws ExecutionException, InterruptedException {
        Msg request = new Msg(values);

        // Receives the transactionId for the given transaction
        CompletableFuture<byte[]> r = ms.sendAndReceive(forwarderAddr, "Client-put", s.encode(request));
        Msg reply = s.decode(r.get());
        int transactionId = (Integer) reply.getData();

        // Add the CF to the map
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        this.putRequests.put(transactionId, cf);

        return cf;
    }

    CompletableFuture<Map<Long,byte[]>> get (Collection<Long> keys) throws ExecutionException, InterruptedException {
        Msg request = new Msg(keys);

        // Receives the transactionId for the given transaction
        CompletableFuture<byte[]> r = ms.sendAndReceive(forwarderAddr, "Client-get", s.encode(request));
        Msg reply = s.decode(r.get());
        int transactionId = (Integer) reply.getData();

        // Add the CF to the map
        CompletableFuture<Map<Long, byte[]>> cf = new CompletableFuture<>();
        this.getRequests.put(transactionId, cf);

        return cf;
    }
}