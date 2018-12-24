import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class GetRequest {
    private int transactionId;
    private String clientAddr;
    private Map<String, Map<Long, byte[]>> participants;
    private Map<String, Collection<Long>> requestedKeys;
    private boolean completed;

    public GetRequest(int transactionId, String clientAddr, Map<Address, Map<Long, byte[]>> participants, Map<Address, Collection<Long>> requestedKeys) {
        this.transactionId = transactionId;
        this.clientAddr = clientAddr;
        Map<String, Map<Long, byte[]>> map = new HashMap<>();
        for (Map.Entry<Address, Map<Long, byte[]>> addressMapEntry : participants.entrySet()) {
            if (addressMapEntry.getValue() == null) {
                map.put(addressMapEntry.getKey().toString(), null);
            } else {
                map.put(addressMapEntry.getKey().toString(), addressMapEntry.getValue());
            }
        }
        this.participants = map;
        this.requestedKeys = requestedKeys.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue()
                ));
        this.completed = false;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public void setClient(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public Map<Address, Map<Long, byte[]>> getParticipants() {
        Map<Address, Map<Long, byte[]>> map = new HashMap<>();
        for (Map.Entry<String, Map<Long, byte[]>> e : participants.entrySet()) {
            if (e.getValue() == null) {
                map.put(Address.from(e.getKey()), null);
            } else {
                map.put(Address.from(e.getKey()), e.getValue());
            }
        }
        return map;
    }

    public void setParticipants(Map<Address, Map<Long, byte[]>> participants) {
        Map<String, Map<Long, byte[]>> map = new HashMap<>();
        for (Map.Entry<Address, Map<Long, byte[]>> addressMapEntry : participants.entrySet()) {
            if (addressMapEntry.getValue() == null) {
                map.put(addressMapEntry.getKey().toString(), null);
            } else {
                map.put(addressMapEntry.getKey().toString(), addressMapEntry.getValue());
            }
        }
        this.participants = map;
    }

    public Map<Address, Collection<Long>> getRequestedKeys() {
        return requestedKeys.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> Address.from(e.getKey()),
                        e -> e.getValue()
                ));
    }

    public void setRequestedKeys(Map<Address, Collection<Long>> requestedKeys) {
        this.requestedKeys = requestedKeys.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue()
                ));
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }
}
