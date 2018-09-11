package ir.sahab.nimroo.elasticsearch;

import ir.sahab.nimroo.model.Meta;
import ir.sahab.nimroo.model.PageData;
import javafx.util.Pair;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.exponentialDecayFunction;

public class ElasticClient {
  private RestHighLevelClient client;
  private BulkRequest request;
  private ArrayList<String> obsceneWords;
  private Logger logger = Logger.getLogger(ElasticClient.class);
  private boolean safeSearch;

  public ElasticClient(String serverAddress) {
    client =
        new RestHighLevelClient(
            RestClient.builder(new HttpHost(serverAddress, 9200, "http"))
                .setRequestConfigCallback(
                    requestConfigBuilder ->
                        requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(600000))
                .setMaxRetryTimeoutMillis(600000));
    request = new BulkRequest();
    safeSearch = false;
  }

  public void createIndexForWebPages(String index) throws IOException {
    GetIndexRequest getIndexRequest = new GetIndexRequest().indices(index);
    if (client.indices().exists(getIndexRequest)) {
      return;
    }
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
    XContentBuilder settings = XContentFactory.jsonBuilder();
    settings.startObject();
    {
      settings.startObject("index");
      {
        settings.field("number_of_shards", 6);
        settings.field("number_of_replicas", 1);
      }
      settings.endObject();
    }
    settings.endObject();
    XContentBuilder mapping = XContentFactory.jsonBuilder();
    mapping.startObject();
    {
      mapping.startObject("_doc");
      {
        mapping.startObject("_source");
        {
          mapping.field("enabled", "true");
        }
        mapping.endObject();
        mapping.startObject("properties");
        {
          mapping.startObject("title");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("text");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("description");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("keywords");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("h1");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("h2");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("anchors");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
        }
        mapping.endObject();
      }
      mapping.endObject();
    }
    mapping.endObject();
    createIndexRequest.mapping("_doc", mapping).settings(settings);
    client.indices().create(createIndexRequest);
  }

  public void createIndexForNews(String index) throws IOException {
    GetIndexRequest getIndexRequest = new GetIndexRequest().indices(index);
    if (client.indices().exists(getIndexRequest)) {
      return;
    }
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
    XContentBuilder settings = XContentFactory.jsonBuilder();
    settings.startObject();
    {
      settings.startObject("index");
      {
        settings.field("number_of_shards", 6);
        settings.field("number_of_replicas", 1);
      }
      settings.endObject();
    }
    settings.endObject();
    XContentBuilder mapping = XContentFactory.jsonBuilder();
    mapping.startObject();
    {
      mapping.startObject("_doc");
      {
        mapping.startObject("_source");
        {
          mapping.field("enabled", "true");
        }
        mapping.endObject();
        mapping.startObject("properties");
        {
          mapping.startObject("title");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("text");
          {
            mapping.field("type", "text");
            mapping.field("analyzer", "english");
            mapping.field("search_analyzer", "english");
            mapping.field("term_vector", "yes");
          }
          mapping.endObject();
          mapping.startObject("pubDate");
          {
            mapping.field("type", "date");
          }
          mapping.endObject();
        }
        mapping.endObject();
      }
      mapping.endObject();
    }
    mapping.endObject();
    createIndexRequest.mapping("_doc", mapping).settings(settings);
    client.indices().create(createIndexRequest);
  }

  public synchronized void addWebPageToBulkOfElastic(PageData pageData, String id, String index)
      throws IOException {
    String url = pageData.getUrl();
    String title = pageData.getTitle();
    String text = pageData.getText();
    String h1 = pageData.getH1();
    StringBuilder h2 = new StringBuilder();
    ArrayList<String> h2List = pageData.getH2();
    if (h2List != null) {
      for (String tempH2List : h2List) {
        h2.append(" ").append(tempH2List);
      }
    }
    String description = null;
    String keywords = null;
    if (pageData.getMetas() != null) {
      for (Meta temp : pageData.getMetas()) {
        if (temp.getName().equals("description")) {
          description = temp.getContent();
        } else if (temp.getName().equals("keywords")) {
          keywords = temp.getContent();
        }
      }
    }
    XContentBuilder builder =
        jsonBuilder()
            .startObject()
            .field("url", url)
            .field("title", title)
            .field("text", text)
            .field("description", description)
            .field("keywords", keywords)
            .field("h1", h1)
            .field("h2", h2.toString())
            .field("anchors", "")
            .endObject();
    request.add(new IndexRequest(index, "_doc", id).source(builder));
  }

  public synchronized void addNewsToBulkOfElastic(
      String url, String title, String text, String pubDate, String id, String index)
      throws IOException {
    XContentBuilder builder =
        jsonBuilder()
            .startObject()
            .field("url", url)
            .field("title", title)
            .field("text", text)
            .field("pubDate", pubDate)
            .endObject();
    request.add(new IndexRequest(index, "_doc", id).source(builder));
  }

  public synchronized void addWebPageUpdateToBulk(PageData pageData, String id, String index)
      throws IOException {
    String url = pageData.getUrl();
    String title = pageData.getTitle();
    String text = pageData.getText();
    String h1 = pageData.getH1();
    StringBuilder h2 = new StringBuilder();
    ArrayList<String> h2List = pageData.getH2();
    if (h2List != null) {
      for (String tempH2List : h2List) {
        h2.append(" ").append(tempH2List);
      }
    }
    String description = null;
    String keywords = null;
    if (pageData.getMetas() != null) {
      for (Meta temp : pageData.getMetas()) {
        if (temp.getName().equals("description") && temp.getContent() != null) {
          description = temp.getContent();
        } else if (temp.getName().equals("keywords") && temp.getContent() != null) {
          keywords = temp.getContent();
        }
      }
    }
    XContentBuilder builder =
        jsonBuilder()
            .startObject()
            .field("url", url)
            .field("title", title)
            .field("text", text)
            .field("description", description)
            .field("keywords", keywords)
            .field("h1", h1)
            .field("h2", h2.toString())
            .endObject();
    request.add(new UpdateRequest(index, "_doc", id).doc(builder));
  }

  public synchronized void addAnchorUpdateToBulk(String anchors, String id, String index)
      throws IOException {
    XContentBuilder builder = jsonBuilder().startObject().field("anchors", anchors).endObject();
    request.add(new UpdateRequest(index, "_doc", id).doc(builder));
  }

  public synchronized void addBulkToElastic() throws IOException {
    if (request.numberOfActions() > 0) {
      client.bulk(request);
      request = new BulkRequest();
    }
  }

  public void readObsceneWordsForSearch() throws URISyntaxException {
    obsceneWords = new ArrayList<>();
    File file = new File(getClass().getClassLoader().getResource("obscene words").toURI());
    Scanner sc = null;
    try {
      sc = new Scanner(file);
    } catch (FileNotFoundException e) {
      logger.error("obscene words file not found", e);
    }
    while (sc.hasNextLine()) {
      obsceneWords.add(sc.nextLine());
    }
  }

  public boolean getSafeSearch() {
    return safeSearch;
  }

  public void setSafeSearch(boolean safety) {
    safeSearch = safety;
  }

  public HashMap<String, Pair<String, Double>> searchInElasticForNews(
      String searchText, String index) throws IOException {
    SearchRequest searchRequest = new SearchRequest(index);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    MultiMatchQueryBuilder multiMatchQueryBuilder =
        QueryBuilders.multiMatchQuery(searchText, "text", "title");
    multiMatchQueryBuilder.field("text", 2);
    multiMatchQueryBuilder.field("title", 1);

    FunctionScoreQueryBuilder functionScoreQueryBuilder =
        new FunctionScoreQueryBuilder(
            multiMatchQueryBuilder, exponentialDecayFunction("pubDate", "now", "240m", "30m"));
    searchSourceBuilder.query(functionScoreQueryBuilder);
    String includes[] = new String[2];
    includes[0] = "url";
    includes[1] = "pubDate";
    searchSourceBuilder.fetchSource(includes, null);
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest);
    SearchHits hits = searchResponse.getHits();
    SearchHit[] searchHits = hits.getHits();
    HashMap<String, Pair<String, Double>> answer = new HashMap<>();
    for (SearchHit hit : searchHits) {
      answer.put(
          (String) hit.getSourceAsMap().get("url"),
          new Pair<>((String) hit.getSourceAsMap().get("pubDate"), (double) hit.getScore()));
    }
    return answer;
  }

  public HashMap<String, Double> simpleSearchInElasticForWebPage(
      String searchText, String index, boolean pageRank) throws IOException {
    SearchRequest searchRequest = new SearchRequest(index);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    if (safeSearch) {
      for (String phrase : obsceneWords) {
        boolQuery.mustNot(
            QueryBuilders.multiMatchQuery(
                    phrase, "text", "title", "description", "keywords", "h1", "h2", "anchors")
                .type(MultiMatchQueryBuilder.Type.PHRASE));
      }
    }
    MultiMatchQueryBuilder multiMatchQueryBuilder =
        QueryBuilders.multiMatchQuery(
            searchText, "text", "title", "description", "keywords", "h1", "h2", "anchors");
    multiMatchQueryBuilder.field("text", 5);
    multiMatchQueryBuilder.field("title", 2);
    multiMatchQueryBuilder.field("description", 1);
    multiMatchQueryBuilder.field("keywords", 1);
    multiMatchQueryBuilder.field("h1", 3);
    multiMatchQueryBuilder.field("h2", 2);
    multiMatchQueryBuilder.field("anchors", 2);
    boolQuery.must(multiMatchQueryBuilder);
    searchSourceBuilder.query(boolQuery);
    if (pageRank) {
      searchSourceBuilder.size(1000);
    }
    searchSourceBuilder.fetchSource("url", null);
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest);
    SearchHits hits = searchResponse.getHits();
    SearchHit[] searchHits = hits.getHits();
    HashMap<String, Double> answer = new HashMap<>();
    for (SearchHit hit : searchHits) {
      answer.put((String) hit.getSourceAsMap().get("url"), (double) hit.getScore());
    }
    return answer;
  }

  public HashMap<String, Double> advancedSearchInElasticForWebPage(
      ArrayList<String> mustFind,
      ArrayList<String> mustNotFind,
      ArrayList<String> shouldFind,
      String index,
      boolean pageRank)
      throws IOException {
    SearchRequest searchRequest = new SearchRequest(index);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    if (safeSearch) {
      for (String phrase : obsceneWords) {
        boolQuery.mustNot(
            QueryBuilders.multiMatchQuery(
                    phrase, "text", "title", "description", "keywords", "h1", "h2", "anchors")
                .type(MultiMatchQueryBuilder.Type.PHRASE));
      }
    }
    for (String phrase : mustFind) {
      MultiMatchQueryBuilder multiMatchQueryBuilder =
          QueryBuilders.multiMatchQuery(
                  phrase, "text", "title", "description", "keywords", "h1", "h2", "anchors")
              .type(MultiMatchQueryBuilder.Type.PHRASE);
      multiMatchQueryBuilder.field("text", 5);
      multiMatchQueryBuilder.field("title", 2);
      multiMatchQueryBuilder.field("description", 1);
      multiMatchQueryBuilder.field("keywords", 1);
      multiMatchQueryBuilder.field("h1", 3);
      multiMatchQueryBuilder.field("h2", 2);
      multiMatchQueryBuilder.field("anchors", 2);
      boolQuery.must(multiMatchQueryBuilder);
    }
    for (String phrase : mustNotFind) {
      boolQuery.mustNot(
          QueryBuilders.multiMatchQuery(
                  phrase, "text", "title", "description", "keywords", "h1", "h2", "anchors")
              .type(MultiMatchQueryBuilder.Type.PHRASE));
    }
    for (String phrase : shouldFind) {
      MultiMatchQueryBuilder multiMatchQueryBuilder =
          QueryBuilders.multiMatchQuery(
                  phrase, "text", "title", "description", "keywords", "h1", "h2", "anchors")
              .type(MultiMatchQueryBuilder.Type.PHRASE);
      multiMatchQueryBuilder.field("text", 5);
      multiMatchQueryBuilder.field("title", 2);
      multiMatchQueryBuilder.field("description", 1);
      multiMatchQueryBuilder.field("keywords", 1);
      multiMatchQueryBuilder.field("h1", 3);
      multiMatchQueryBuilder.field("h2", 2);
      multiMatchQueryBuilder.field("anchors", 2);
      boolQuery.should(multiMatchQueryBuilder);
    }
    searchSourceBuilder.query(boolQuery);
    if (pageRank) {
      searchSourceBuilder.size(1000);
    }
    searchSourceBuilder.fetchSource("url", null);
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest);
    SearchHits hits = searchResponse.getHits();
    SearchHit[] searchHits = hits.getHits();
    HashMap<String, Double> answer = new HashMap<>();
    for (SearchHit hit : searchHits) {
      answer.put((String) hit.getSourceAsMap().get("url"), (double) hit.getScore());
    }
    return answer;
  }

  public void closeClient() throws IOException {
    client.close();
  }
}
