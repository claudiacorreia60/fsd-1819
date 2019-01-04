import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Client {
    public static void main(String[] args) throws Exception {
        ClientStub client = new ClientStub(args[0], args[1],Integer.parseInt(args[2]));

        Map<Long, byte[]> values = new HashMap();
        // Add values to the Map
        values.put((long) 1111, "Hello".getBytes());
        values.put((long) 2, "its".getBytes());
        values.put((long) 3, "me".getBytes());


        Collection<Long> keys = new ArrayList();
        // Add values to the collection
        keys.add((long) 1111);
        keys.add((long) 2);
        keys.add((long) 3);
        keys.add((long) 4);

        /*
        client.put(values).thenCompose((b) -> {
            System.out.println(b);
            try {
                return client.get(keys).thenCompose((r) -> {
                    System.out.println(r.keySet().toString());
                    r.values().forEach(bytes -> System.out.println(new String(bytes)));
                    return new CompletableFuture<>();
                });
            } catch (ExecutionException e) {
                return new CompletableFuture<>();
            } catch (InterruptedException e) {
                return new CompletableFuture<>();
            }
        });
        */

        client.get(keys).thenCompose((r) -> {
            System.out.println(r.keySet().toString());
            r.values().forEach(bytes -> System.out.println(new String(bytes)));
            return new CompletableFuture<>();
        });
    }
}
