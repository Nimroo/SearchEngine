package ir.sahab.nimroo.searchapi;

import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcMethod;
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcParam;
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcService;
import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.elasticsearch.SearchUIConnector;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@JsonRpcService
public class JsonRpcSearchService {
    SearchUIConnector searchUIConnector;

    public JsonRpcSearchService(SearchUIConnector searchUIConnector) {
        this.searchUIConnector = searchUIConnector;
    }

    @JsonRpcMethod
    public String ping() {
        //TODO ali Sout
//        System.out.println("ping called");
        return "pong";
    }

    @JsonRpcMethod
    public List<Pair> normalSearch(
            @JsonRpcParam("text")final String text,
            @JsonRpcParam("safety")final boolean safety,
            @JsonRpcParam("pageRank")final boolean pageRank) {
        try {
            return searchUIConnector.simpleSearch(text, Config.elasticsearchIndexName, safety, pageRank).stream()
                    .map(item -> new Pair(item, "")).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList();
        }
    }

    @JsonRpcMethod
    public List<Pair> advanceSearch(
            @JsonRpcParam("must")final ArrayList<String> must,
            @JsonRpcParam("must_not")final ArrayList<String> mustNot,
            @JsonRpcParam("should")final ArrayList<String> should,
            @JsonRpcParam("safety")final boolean safety,
            @JsonRpcParam("pageRank")final boolean pageRank) {
        try {
            return searchUIConnector.advancedSearch(must, mustNot, should,
                    Config.elasticsearchIndexName, safety, pageRank).stream()
                    .map(item -> new Pair(item, "")).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    @JsonRpcMethod
    public List<Pair> newsSearch(@JsonRpcParam("query")final String query) throws Exception {

        try {
            return new ArrayList<>(searchUIConnector.newsSearch(query, "newsindex"));
        }
        catch (Exception e) {
            System.out.println(e);
        }
        throw new Exception("shit");
    }

}
