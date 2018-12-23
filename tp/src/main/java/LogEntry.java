import java.util.List;
import java.util.Map;

public class LogEntry {
    public String entryType;
    public int transactionId;
    public List<String> participants;
    public String participant;
    public String forwarderAddr;
    public Map<Long, byte[]> pairs;


    // Manager - Initialized
    public LogEntry(String entryType, int transactionId, List<String> participants) {
        this.entryType = entryType;
        this.transactionId = transactionId;
        this.participants = participants;

        this.participant = null;
        this.forwarderAddr = null;
        this.pairs = null;
    }

    // Server - Initialized
    public LogEntry(String entryType, int transactionId, String forwarderAddr, Map<Long, byte[]> pairs) {
        this.entryType = entryType;
        this.transactionId = transactionId;
        this.forwarderAddr = forwarderAddr;
        this.pairs = pairs;

        this.participants = null;
        this.participant = null;
    }

    // Manager - Prepared
    public LogEntry(String entryType, int transactionId, String participant) {
        this.entryType = entryType;
        this.transactionId = transactionId;
        this.participant = participant;

        this.participants = null;
        this.forwarderAddr = null;
        this.pairs = null;
    }

    // Server - Prepared
    // Manager and Server - Committed
    // Manager and Server - Aborted
    public LogEntry(String entryType, int transactionId) {
        this.entryType = entryType;
        this.transactionId = transactionId;

        this.participants = null;
        this.participant = null;
        this.forwarderAddr = null;
        this.pairs = null;
    }
}