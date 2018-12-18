import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import java.util.concurrent.CompletableFuture;


public class Stub {
    private final ManagedMessagingService ms;
    private final Address srv;
    private final Serializer s;


    public Stub (InetAddress address, int port) throws Exception {
        srv = Address.from(address, port);
        ms = NettyMessagingService.builder().withAddress(Address.from(10000)).build();
        s = Serializer.builder()
                .withTypes(
                    Msg.class)
                .build();
        ms.start().get();
    }

    @Override
    CompletableFuture<Boolean> put (Map<Long,byte[]> values) {
        Msg request = new Msg(values);
        CompletableFuture<byte[]> r = ms.sendAndReceive(srv, "Client-put", s.encode(request));
        Msg reply = s.decode(r.get());
        return (CompletableFuture<Boolean>) reply.getData();
    }

    @Override
    CompletableFuture<Map<Long,byte[]>> get (Collection<Long> keys) {
        Msg request = new Msg(keys);
        CompletableFuture<byte[]> r = ms.sendAndReceive(srv, "Client-get", s.encode(request));
        Msg reply = s.decode(r.get());
        return (CompletableFuture<Map<Long,byte[]>>) reply.getData();
    }
}