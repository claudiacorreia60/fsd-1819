import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

    /*
        TODO: Add instance variables related with the 2PC
            - Ver guiao 5
     */

    private Serializer s;
    private ManagedMessagingService ms;
    private ExecutorService es;
    private Address managerAddr;
    private Map<Long, byte[]> pairs;


    public Server(String myAddress, Address managerAddr) {
        this.s = Serializer.builder()
                .withTypes(
                        Msg.class)
                .build();
        this.ms = NettyMessagingService.builder().withAddress(Address.from(myAddress)).build();
        this.es = Executors.newSingleThreadExecutor();
        this.managerAddr = managerAddr;
        this.pairs = new HashMap<>();

        /* -----  FORWARDER -> SERVER ----- */

        // Receive forwarder put request
        this.ms.registerHandler("Forwarder-put", (o, m) -> {
            Msg msg = this.s.decode(m);
            Map<Integer, Map<Long, byte[]>> response = (Map<Integer, Map<Long, byte[]>>) msg.getData();

            // TODO: Handle the put request along with the 2PC

        }, this.es);

        // Receive forwarder get request
        this.ms.registerHandler("Forwarder-get", (o, m) -> {
            Msg msg = this.s.decode(m);
            Collection<Long> response = (Collection<Long>) msg.getData();

            // Search for the asked pairs
            Map<Long, byte[]> responsePairs = new HashMap<>();
            for (Map.Entry<Long, byte[]> e : this.pairs.entrySet()) {
                if (response.stream().anyMatch(k -> k == e.getKey())) {
                    responsePairs.put(e.getKey(), e.getValue());
                }
            }

            msg = new Msg(responsePairs);
            this.ms.sendAsync(o, "Server-get", this.s.encode(msg));
        }, this.es);

        /* TODO: Handle 2PC with Manager */

    }
}
