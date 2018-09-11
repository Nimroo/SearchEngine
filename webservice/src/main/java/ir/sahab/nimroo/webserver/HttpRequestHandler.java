package ir.sahab.nimroo.webserver;

import com.github.arteam.simplejsonrpc.server.JsonRpcServer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

class HttpRequestHandler extends AbstractHandler {
    JsonRpcServer rpcServer = new JsonRpcServer();
    NimrooJsonRpcService jsonRpcSearchService;

    public HttpRequestHandler(NimrooJsonRpcService jsonRpcService) {
        this.jsonRpcSearchService = jsonRpcService;
    }

    private String handleRpc(String json) {
        return rpcServer.handle(json, jsonRpcSearchService);
    }

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        StringBuilder sb = new StringBuilder();
        String line = "";
        while (line != null) {
            sb.append(line);
            line = request.getReader().readLine();
        }

        System.out.println(target);

        response.setHeader("Access-Control-Allow-Headers",
                "Origin, Content-Type, X-Auth-Token, Access-Control-Allow-Origin");
        response.setHeader("Access-Control-Allow-Methods", "GET");
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.getWriter().print(handleRpc(sb.toString()));
    }

}
