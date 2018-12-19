import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;


public class Manager {
    private Serializer s;
    private ManagedMessagingService ms;
    private ScheduledExecutorService es;
    private int transactionId;
    private Map<Integer, Set<Address>> participants; // ID da transação, Lista de participantes


    public Manager(ManagedMessagingService ms, ScheduledExecutorService es, Address forwarderAddr) {
        this.s = Serializer.builder()
                .withTypes(
                        Msg.class)
                .build();
        this.ms = ms;
        this.es = es;
        this.transactionId = 0;


        /* -----  FORWARDER -> MANAGER ----- */

        // Receive forwarder begin transaction
        this.ms.registerHandler("Forwarder-begin", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Handle forwarder begin
            contextHandler((Set<Address>) msg.getData(), o);
        }, this.es);
    }


    public void contextHandler(Set<Address> participantServers, Address forwarder) {
        // Save participant servers
        this.participants.put(this.transactionId, participantServers);

        //Send transaction id to forwarder
        Msg msg = new Msg(this.transactionId);
        this.ms.sendAsync(forwarder, "Manager-context", this.s.encode(msg));
        this.transactionId++;
    }


    /* TODO: 2PC with Server */
}