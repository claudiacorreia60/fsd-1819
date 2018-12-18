import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Client {

    public static void main(String[] args) throws Exception {
        Stub client = new Stub(args[0], Integer.parseInt(args[1]));

        Map<Long, byte[]> values = new HashMap();
        // Adicionar valores ao map
        Collection<Long> keys = new ArrayList();
        // Adicionar valores à collection

        CompletableFuture<Boolean> result_put = client.put(values);
        CompletableFuture<Map<Long, byte[]>> result_get = client.get(keys);
    }
}
