import io.atomix.utils.net.Address;

import java.util.Map;

public class PutRequest {
    private int transactionId;
    private Map<Address, Integer> participants; // Integer -> 0-SR, 1-S, 2-N
    private Address client;

    public PutRequest(int transactionId, Map<Address, Integer> participants, Address client) {
        this.transactionId = transactionId;
        this.participants = participants;
        this.client = client;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public Map<Address, Integer> getParticipants() {
        return participants;
    }

    public Address getClient() {
        return client;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public void setParticipants(Map<Address, Integer> participants) {
        this.participants = participants;
    }

    public void setClient(Address client) {
        this.client = client;
    }
}
