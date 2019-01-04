import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PutRequest {
    private int transactionId;
    private String clientAddr;
    private Map<String, Integer> participants; // Integer -> 0-SR, 1-S, 2-N
    private Map<String, Map<Long, byte[]>> keysToPut; // Integer -> 0-SR, 1-S, 2-N
    private boolean completed;
    private boolean sent;

    public PutRequest(int transactionId, String clientAddr, Map<Address, Integer> participants, Map<Address, Map<Long, byte[]>> keysToPut) {
        this.transactionId = transactionId;
        this.clientAddr = clientAddr;
        this.participants = participants.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue()
                ));
        this.keysToPut = keysToPut.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue()
                ));
        this.completed = false;
        this.sent = false;
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

    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public Map<Address, Integer> getParticipants() {
        return participants.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> Address.from(e.getKey()),
                        e -> e.getValue()
                ));
    }

    public void setParticipants(Map<Address, Integer> participants) {
        this.participants = participants.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue()
                ));
    }

    public Map<Address, Map<Long, byte[]>> getKeysToPut() {
        return keysToPut.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> Address.from(e.getKey()),
                        e -> e.getValue()
                ));
    }

    public void setKeysToPut(Map<Address, Map<Long, byte[]>> keysToPut) {
        this.keysToPut = keysToPut.entrySet().stream()
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

    public boolean isSent() {
        return sent;
    }

    public void setSent(boolean sent) {
        this.sent = sent;
    }
}
