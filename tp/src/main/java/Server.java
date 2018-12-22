import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

    /*
        TODO: Add instance variables related with the 2PC
            - Ver guiao 5
     */

    private Log log;
    private Serializer s;
    private ManagedMessagingService ms;
    private ExecutorService es;
    private Address managerAddr;
    private Map<Long, byte[]> pairs;
    private Map<Integer, Map.Entry<Address, Map<Long, byte[]>>> pairsVolatile;

    public Server(String myAddress, Address managerAddr) throws ExecutionException, InterruptedException {
        this.log = new Log(myAddress);
        this.log.open(0);
        this.s = Serializer.builder()
                .withTypes(
                        Msg.class)
                .build();
        this.ms = NettyMessagingService.builder().withAddress(Address.from(myAddress)).build();
        this.es = Executors.newSingleThreadExecutor();
        this.managerAddr = managerAddr;
        this.pairs = new HashMap<>();
        this.pairsVolatile = new HashMap<>();

        this.loadLog();

        /* -----  MANAGER -> SERVER ----- */

        // Receive 2PC Prepared question from manager
        this.ms.registerHandler("Manager-prepared", (o, m) -> {
            Msg msg = this.s.decode(m);

            int transactionId = (Integer) msg.getData();

            // Reply to manager if
            if (this.pairsVolatile.containsKey(transactionId)) {

                // Write Prepared to log
                LogEntry le = new LogEntry("Prepared", transactionId);
                this.log.append(le);

                // Reply to manager
                this.ms.sendAsync(o, "Server-prepared", m);
            } else {
                // Write Abort to log
                LogEntry le = new LogEntry("Abort", transactionId);
                this.log.append(le);

                // Reply to manager
                this.ms.sendAsync(o, "Server-abort", m);
            }

        }, this.es);

        // Receive 2PC Commit message from manager
        this.ms.registerHandler("Manager-commit", (o, m) -> {
            Msg msg = this.s.decode(m);

            int transactionId = (Integer) msg.getData();

            // Commit Key-Value pairs to DB
            Map.Entry<Address, Map<Long, byte[]>> keysToPut = this.pairsVolatile.get(transactionId);
            this.pairs.putAll(keysToPut.getValue());
            this.pairsVolatile.remove(transactionId);

            // Write Commit to log
            LogEntry le = new LogEntry("Commit", transactionId);
            this.log.append(le);

            // Inform forwarder that the transaction is completed
            Address forwarderAddr = keysToPut.getKey();
            this.ms.sendAsync(forwarderAddr, "Server-true", this.s.encode(msg));

        }, this.es);

        // Receive 2PC Commit message from manager
        this.ms.registerHandler("Manager-abort", (o, m) -> {
            Msg msg = this.s.decode(m);

            int transactionId = (Integer) msg.getData();

            Address forwarderAddr = this.pairsVolatile.get(transactionId).getKey();
            // Delete Key-Value pairs from volatile DB
            this.pairsVolatile.remove(transactionId);

            // Write Abord to log
            LogEntry le = new LogEntry("Abort", transactionId);
            this.log.append(le);

            // Inform forwarder that the transaction is completed
            this.ms.sendAsync(forwarderAddr, "Server-false", this.s.encode(msg));

        }, this.es);

        /* -----  FORWARDER -> SERVER ----- */

        // Receive forwarder put request
        this.ms.registerHandler("Forwarder-put", (o, m) -> {
            Msg msg = this.s.decode(m);
            Map.Entry<Integer, Map<Long, byte[]>> response = (Map.Entry<Integer, Map<Long, byte[]>>) msg.getData();

            // Write new transaction to log
            LogEntry le = new LogEntry("Initialized", response.getKey(), o, response.getValue());
            this.log.append(le);

            // Add to volatile DB
            Map.Entry<Address, Map<Long, byte[]>> newEntry = new AbstractMap.SimpleEntry<>(o, response.getValue());
            this.pairsVolatile.put(response.getKey(), newEntry);

            // Write new state for this transaction to log
            le = new LogEntry("Prepared", response.getKey());
            this.log.append(le);

            // Tell Manager that the server is prepared
            msg = new Msg(response.getKey());
            this.ms.sendAsync(this.managerAddr, "Server-prepared", this.s.encode(msg));

        }, this.es);

        // Receive forwarder get request
        // TODO: Pensar em registar no log os pedidos de get em caso de erro
        this.ms.registerHandler("Forwarder-get", (o, m) -> {
            Msg msg = this.s.decode(m);
            Map.Entry<Integer, Collection<Long>> response = (Map.Entry<Integer, Collection<Long>>) msg.getData();

            // Search for the asked pairs
            Map<Long, byte[]> responsePairs = new HashMap<>();
            for (Map.Entry<Long, byte[]> e : this.pairs.entrySet()) {
                if (response.getValue().stream().anyMatch(k -> k == e.getKey())) {
                    responsePairs.put(e.getKey(), e.getValue());
                }
            }

            // Reply to forwarder
            Map.Entry<Long, byte[]> result = new AbstractMap.SimpleEntry(response.getKey(), responsePairs);
            msg = new Msg(result);
            this.ms.sendAsync(o, "Server-get", this.s.encode(msg));
        }, this.es);

        this.ms.start().get();
    }

    // Performs certain actions based on the state of a given transaction
    private void validateLog(Map<Integer, String> transactions){
        for(Map.Entry<Integer, String> e : transactions.entrySet()) {
           if (e.getValue().equals("Prepared")) {
               Msg msg = new Msg(e.getKey());
               this.ms.sendAsync(this.managerAddr, "Server-prepared", this.s.encode(msg));
           }
           if (e.getValue().equals("Abort")) {
               this.pairsVolatile.remove(e.getKey());
           }
        }
    }

    // Parses the log
    private void loadLog() {
        SegmentedJournalReader<Object> r = this.log.read();

        Map<Integer, String> transactions = new HashMap<>();

        while(r.hasNext()) {
            LogEntry le = (LogEntry) r.next().entry();
            int transactionId = le.transactionId;
            if (le.entryType.equals("Initialized")) {
               this.pairsVolatile.put(transactionId, new AbstractMap.SimpleEntry<>(le.forwarderAddr, le.pairs));
               transactions.put(transactionId, "Initialized");
            }
            if (le.entryType.equals("Prepared")) {
                transactions.put(transactionId, "Prepared");
            }
            if (le.entryType.equals("Commit")) {
                transactions.put(transactionId, "Commit");
            }
            if (le.entryType.equals("Abort")) {
                transactions.put(transactionId, "Abort");
            }
        }

        this.validateLog(transactions);
    }

}
