package ir.sahab.nimroo.searchapi;

import ir.sahab.nimroo.elasticsearch.SearchUIConnector;
import ir.sahab.nimroo.Config;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        Config.load();
        PropertyConfigurator.configure(ServerLauncher.class.getClassLoader().getResource("log4j.properties"));

        Server server = new Server(6060);
        SearchUIConnector searchUIConnector = new SearchUIConnector();
        JsonRpcSearchService jsonRpcSearchService = new JsonRpcSearchService(searchUIConnector);
        server.setHandler(new HttpRequestHandler(jsonRpcSearchService));

        server.start();
        server.join();
    }

}
