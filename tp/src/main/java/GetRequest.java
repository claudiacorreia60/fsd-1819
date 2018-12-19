import io.atomix.utils.net.Address;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GetRequest {
    private int transactionId;
    private Map<Address, Map<Long, byte[]>> participants;
    private CompletableFuture<byte[]> cf;

    public GetRequest(int transactionId, Map<Address, Map<Long, byte[]>> participants, CompletableFuture<byte[]> cf) {
        this.transactionId = transactionId;
        this.participants = participants;
        this.cf = cf;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public Map<Address, Map<Long, byte[]>> getParticipants() {
        return participants;
    }

    public void setParticipants(Map<Address, Map<Long, byte[]>> participants) {
        this.participants = participants;
    }

    public CompletableFuture<byte[]> getCf() {
        return cf;
    }

    public void setCf(CompletableFuture<byte[]> cf) {
        this.cf = cf;
    }
}
