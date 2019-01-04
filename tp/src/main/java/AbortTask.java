import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

public class AbortTask extends TimerTask {
    private int transactionId;
    private Serializer s;
    private ManagedMessagingService ms;
    private ExecutorService es;
    private Map<Integer, Map<Address, Integer>> participants; // <transaction ID, <participant, reply>>
    private Map<Integer, Boolean> transactionsState; // <transaction ID, isTransactionCompleted>
    private Log log;

    public AbortTask(int transactionId, Serializer s, ManagedMessagingService ms, ExecutorService es, Map<Integer, Map<Address, Integer>> participants, Map<Integer, Boolean> transactionsState, Log log) {
        this.transactionId = transactionId;
        this.s = s;
        this.ms = ms;
        this.es = es;
        this.participants = participants;
        this.transactionsState = transactionsState;
        this.log = log;
    }

    @Override
    public void run() {

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
            }
        }
    }
}
