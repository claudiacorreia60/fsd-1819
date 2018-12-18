import io.atomix.utils.net.Address;

import java.util.Map;

public class GetRequest {
    private int transactionId;
    private Map<Address, Map<Long, byte[]>> participants;
    private Address client;

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

    public Address getClient() {
        return client;
    }

    public void setClient(Address client) {
        this.client = client;
    }

    public GetRequest(int transactionId, Map<Address, Map<Long, byte[]>> participants, Address client) {
        this.transactionId = transactionId;
        this.participants = participants;
        this.client = client;
    }
}
