import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class Stub {
    private final ManagedMessagingService ms;
    private final Address srv;
    private final Serializer s;


    public Stub (String myAddress, String address, int port) throws Exception {
        srv = Address.from(address, port);
        ms = NettyMessagingService.builder().withAddress(Address.from(myAddress)).build();
        s = Serializer.builder()
                .withTypes(
                    Msg.class)
                .build();
        ms.start().get();
    }

    CompletableFuture<Boolean> put (Map<Long, byte[]> values) throws ExecutionException, InterruptedException {
        Msg request = new Msg(values);
        CompletableFuture<byte[]> r = ms.sendAndReceive(srv, "Client-put", s.encode(request));
        Msg reply = s.decode(r.get());

        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        try {
            boolean b = (Boolean) reply.getData();
            cf.complete(b);
        } catch (Exception e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    CompletableFuture<Map<Long,byte[]>> get (Collection<Long> keys) throws ExecutionException, InterruptedException {
        Msg request = new Msg(keys);
        CompletableFuture<byte[]> r = ms.sendAndReceive(srv, "Client-get", s.encode(request));
        Msg reply = s.decode(r.get());

        CompletableFuture<Map<Long, byte[]>> cf = new CompletableFuture<>();
        try {
            Map<Long, byte[]> pairs = (HashMap<Long, byte[]>) reply.getData();
            cf.complete(pairs);
        } catch (Exception e) {
            cf.completeExceptionally(e);
        }

        return cf;
    }
}