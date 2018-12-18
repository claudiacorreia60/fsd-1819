public class Client {

    public static void main(String[] args) throws Exception {
        Stub client = new Stub();

        Map<Long,byte[]> values = new HashMap();
        // Adicionar valores ao map
        Collection<Long> keys = new Collection();
        // Adicionar valores à collection

        CompletableFuture<Boolean> result_put = client.put(values)
        CompletableFuture<Map<Long,byte[]>> result_get = client.get(keys)
    }
}
