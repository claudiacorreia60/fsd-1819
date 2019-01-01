import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


public class Manager {
    private Serializer s;
    private ManagedMessagingService ms;
    private ExecutorService es;
    private int transactionId;
    private Map<Integer, Map<Address, Boolean>> participants; // <transaction ID, <participant, reply>>
    private Map<Integer, Boolean> transactionsState;
    private Map<Integer, Timer> transactionsTimer;
    private Log log;


    public Manager(Address myAddr) throws ExecutionException, InterruptedException {
        this.s = Serializer.builder()
                .withTypes(
                        Msg.class,
                        AbstractMap.SimpleEntry.class,
                        PutRequest.class,
                        GetRequest.class)
                .build();
        this.ms = NettyMessagingService.builder().withAddress(myAddr).build();
        this.es = Executors.newSingleThreadExecutor();
        this.transactionId = 0;
        this.participants = new HashMap<>();
        this.transactionsState = new HashMap<>();
        this.transactionsTimer = new HashMap<>();
        this.log = new Log("manager");
        this.loadLog();


        /* -----  FORWARDER -> MANAGER ----- */

        // Receive forwarder begin transaction
        this.ms.registerHandler("Forwarder-begin", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Handle forwarder begin
            List<Address> data = ((List<String>) msg.getData()).stream().map(s -> Address.from(s)).collect(Collectors.toList());
            Set<Address> participants = new HashSet<>(data);
            return beginTransactionHandler(participants, o);
        });

        // Receive forwarder begin transaction
        this.ms.registerHandler("Forwarder-isTransactionReady", (o, m) -> {
            Msg msg = this.s.decode(m);

            // Check if transaction is ready
            int transactionId = (Integer) msg.getData();
            Map<Address, Boolean> participants = this.participants.get(transactionId);

            // If transaction was commited answer Forwarder immediately
            // Otherwise begin disaster recovery
            if (participants.values().stream().allMatch(b -> b == true)) {
                Map.Entry<Integer, Boolean> answer = new AbstractMap.SimpleEntry<>(transactionId, true);
                msg = new Msg(answer);
               this.ms.sendAsync(o, "Manager-transactionIsReady", this.s.encode(msg));
            } else {
                for (Address a : participants.keySet()) {
                    this.ms.sendAsync(a, "Manager-prepared", m);
                }
            }


        }, this.es);

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


    public CompletableFuture<byte[]> beginTransactionHandler(Set<Address> participantServers, Address forwarderAddr) {
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

        // Transaction initiated as uncompleted
        this.transactionsState.put(this.transactionId, false);

        // Start AbortTask for transactions that timed out
        Timer t = new Timer();
        TimerTask abortTask = new AbortTask(this.transactionId, this.s, this.ms, this.es, this.participants, this.transactionsState, this.log);
        // Abort after 10 seconds
        t.schedule(abortTask, 10000);
        this.transactionsTimer.put(this.transactionId, t);

        // Increment transaction ID
        this.transactionId++;

        // Complete cf
        CompletableFuture<byte[]> cf = CompletableFuture.completedFuture(
                this.s.encode(new Msg(this.transactionId - 1))
        );

        return cf;
    }

    public void commit(int transactionId){

        // Synchronize with AbortTask
        Boolean isTransactionCompleted = this.transactionsState.get(transactionId);

        synchronized(isTransactionCompleted){
            if(!isTransactionCompleted) {
                // Store committed transaction in log
                LogEntry le = new LogEntry("Committed", transactionId);
                this.log.append(le);

                // Send commit message to participant servers
                for (Address a : this.participants.get(transactionId).keySet()) {
                    Msg msg = new Msg(transactionId);
                    ms.sendAsync(a, "Manager-commit", this.s.encode(msg));
                }

                // Set transaction as completed
                this.transactionsState.put(transactionId, true);

                // Stop and remove abort task
                this.transactionsTimer.get(transactionId).cancel();
                this.transactionsTimer.get(transactionId).purge();
                this.transactionsTimer.remove(transactionId);
            }
        }
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
        
        // Synchronize with AbortTask
        Boolean isTransactionCompleted = this.transactionsState.get(transactionId);

        synchronized(isTransactionCompleted){
            if(!isTransactionCompleted) {
                // Send abort message to participant servers
                for(Address a : this.participants.get(transactionId).keySet()){
                    Msg msg = new Msg(transactionId);
                    ms.sendAsync(a, "Manager-abort", this.s.encode(msg));
                }

                // Store aborted transaction in log
                LogEntry le = new LogEntry("Aborted", transactionId);
                this.log.append(le);

                // Set transaction as completed
                this.transactionsState.put(transactionId, true);

                // Stop and remove abort task
                this.transactionsTimer.get(transactionId).cancel();
                this.transactionsTimer.get(transactionId).purge();
                this.transactionsTimer.remove(transactionId);
            }
        }
    }


    public void validateLog(Map<Integer, String> transactions){
        for(Map.Entry<Integer, String> e : transactions.entrySet()){
            if(e.getValue().equals("Initialized")){
                for(Address a : this.participants.get(e.getKey()).keySet()){
                    Msg msg = new Msg(e.getKey());
                    this.ms.sendAsync(a, "Manager-prepared",this.s.encode(msg));
                }

                // Start AbortTask for transactions that timed out
                Timer t = new Timer();
                TimerTask abortTask = new AbortTask(e.getKey(), this.s, this.ms, this.es, this.participants, this.transactionsState, this.log);
                // Abort after 10 seconds
                t.schedule(abortTask, 10000);
                this.transactionsTimer.put(e.getKey(), t);
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