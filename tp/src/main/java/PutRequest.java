import io.atomix.utils.net.Address;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PutRequest {
    private int transactionId;
    private Map<Address, Integer> participants; // Integer -> 0-SR, 1-S, 2-N
    private CompletableFuture<byte[]> cf;

    public PutRequest(int transactionId, Map<Address, Integer> participants, CompletableFuture<byte[]>  cf) {
        this.transactionId = transactionId;
        this.participants = participants;
        this.cf = cf;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public Map<Address, Integer> getParticipants() {
        return participants;
    }

    public CompletableFuture<byte[]>  getCf() {
        return cf;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public void setParticipants(Map<Address, Integer> participants) {
        this.participants = participants;
    }

    public void setCf(CompletableFuture<byte[]>  cf) {
        this.cf = cf;
    }
}
