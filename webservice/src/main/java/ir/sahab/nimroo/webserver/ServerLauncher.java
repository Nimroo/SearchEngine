package ir.sahab.nimroo.webserver;

import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.elasticsearch.SearchUIConnector;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        Config.load();
        PropertyConfigurator.configure(ServerLauncher.class.getClassLoader().getResource("log4j.properties"));

        Server server = new Server(6060);
        SearchUIConnector searchUIConnector = new SearchUIConnector();
        NimrooJsonRpcService jsonRpcSearchService = new NimrooJsonRpcService(searchUIConnector);

        HttpRequestHandler reqHandler = new HttpRequestHandler(jsonRpcSearchService);

        server.setHandler(reqHandler);

        server.start();
        server.join();
    }

}
