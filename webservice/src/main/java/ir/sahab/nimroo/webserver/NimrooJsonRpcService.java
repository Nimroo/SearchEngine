package ir.sahab.nimroo.webserver;

import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcMethod;
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcParam;
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcService;
import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.elasticsearch.SearchUIConnector;
import ir.sahab.nimroo.hbase.DomainRepository;
import ir.sahab.nimroo.hbase.NewsRepository;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@JsonRpcService
public class NimrooJsonRpcService {
    SearchUIConnector searchUIConnector;
    private Configuration hBaseConfiguration;
    private String domainTableString, domainFamilyString, reverseDomainTableString, reversrDomainFamilyString;
    private Table domainTable, reverseDomainTable;
    private Connection connection;
    private Logger logger;


    public NimrooJsonRpcService(SearchUIConnector searchUIConnector) {
        this.searchUIConnector = searchUIConnector;
    }

    @JsonRpcMethod
    public String ping() {
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

    @JsonRpcMethod
    public List<Pair> keywords() throws IOException {
        return NewsRepository.getInstance().getTop10Trends().stream()
                .map(item -> new Pair(item, "")).collect(Collectors.toList());
    }

    @JsonRpcMethod
    public List<Pair> getSink(@JsonRpcParam("domain") final String domain) {
        return DomainRepository.getInstance().getSinkDomains(domain).stream()
                .map(item -> new Pair(item.getFirst(), item.getSecond())).collect(Collectors.toList());
    }


    @JsonRpcMethod
    public List<Pair> getSource(@JsonRpcParam("domain") final String domain) {
        return DomainRepository.getInstance().getSourceDomains(domain).stream()
                .map(item -> new Pair(item.getFirst(), item.getSecond())).collect(Collectors.toList());
    }

}
