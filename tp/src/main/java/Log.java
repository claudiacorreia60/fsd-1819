import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

public class Log {

    private int transactionId;
    private Serializer s;
    private SegmentedJournal<Object> j;
    private SegmentedJournalReader<Object> r;
    private SegmentedJournalWriter<Object> w;

    public Log(String id) {
        this.s = Serializer.builder()
                .withTypes(LogEntry.class)
                .build();

        this.j = SegmentedJournal.builder()
                .withName("log-"+id)
                .withSerializer(s)
                .build();

        this.r = j.openReader(0);

    }

    public void append(LogEntry le) {
        this.w = j.writer();

        this.w.append(le);
        this.w.flush();
    }

    public void open(int index) {
        this.r = this.j.openReader(index);
    }

    public SegmentedJournalReader<Object> read() {
        return this.r;
    }
}
