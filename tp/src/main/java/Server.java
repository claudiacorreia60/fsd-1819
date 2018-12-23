public class Server {
    public static void main(String[] args) throws Exception {
        ServerSkeleton server = new ServerSkeleton(args[0], args[1], Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]));
    }
}
