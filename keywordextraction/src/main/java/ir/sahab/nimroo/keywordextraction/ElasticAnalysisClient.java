package ir.sahab.nimroo.keywordextraction;

import javafx.util.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ElasticAnalysisClient {
  private RestClient client;

  public ElasticAnalysisClient(String serverAddress) {
    client =
        RestClient.builder(new HttpHost(serverAddress, 9200, "http"))
            .setRequestConfigCallback(
                requestConfigBuilder ->
                    requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(600000))
            .setMaxRetryTimeoutMillis(600000)
            .build();
  }

  public HashMap<String, Double> getInterestingKeywords(
      String index, String id, int numberOfKeywords) throws IOException {
    Map<String, String> params = Collections.emptyMap();
    String jsonString =
        "{"
            + "\"fields\" : [\"text\"],"
            + "\"term_statistics\" : true,"
            + "\"field_statistics\" : true,"
            + "\"positions\" : false,"
            + "\"offsets\" : false,"
            + "\"filter\": {"
            + "\"max_num_terms\" : "
            + numberOfKeywords
            + "}"
            + "}";
    HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
    Response response =
        client.performRequest("GET", "/" + index + "/_doc/" + id + "/_termvectors", params, entity);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseString = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      responseString.append(line);
    }
    JSONObject responseObject = new JSONObject(responseString.toString());
    JSONObject termsObject =
        responseObject.getJSONObject("term_vectors").getJSONObject("text").getJSONObject("terms");
    HashMap<String, Double> ans = new HashMap<>();
    double maxScore = 0;
    for (String key : termsObject.keySet()) {
      double score = (double) termsObject.getJSONObject(key).get("score");
      if (score > maxScore) {
        maxScore = score;
      }
      ans.put(key, score);
    }
    for (HashMap.Entry<String, Double> temp : ans.entrySet()) {
      temp.setValue(temp.getValue() / maxScore);
    }
    return ans;
  }

  public List<Pair<String, List<Pair<String, Double>>>>
      getInterestingKeywordsForMultiDocuments(
          String index, List<String> ids, int numberOfKeywords) throws IOException {
    Map<String, String> params = Collections.emptyMap();
    StringBuilder idsStringbuilder = new StringBuilder();
    for (int i = 0; i < ids.size(); i++) {
      if (i < ids.size() - 1) {
        idsStringbuilder.append("\"").append(ids.get(i)).append("\",");
      } else {
        idsStringbuilder.append("\"").append(ids.get(i)).append("\"");
      }
    }

    String jsonString =
        "{"
            + "\"ids\" : ["
            + idsStringbuilder.toString()
            + "],"
            + "\"parameters\" : {"
            + "\"fields\" : [\"text\"],"
            + "\"term_statistics\" : true,"
            + "\"field_statistics\" : true,"
            + "\"positions\" : false,"
            + "\"offsets\" : false,"
            + "\"filter\": {"
            + "\"max_num_terms\" : "
            + numberOfKeywords
            + "}"
            + "}"
            + "}";
    HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
    Response response =
        client.performRequest("GET", "/" + index + "/_doc/_mtermvectors", params, entity);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseString = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      responseString.append(line);
    }
    JSONObject responseObject = new JSONObject(responseString.toString());
    JSONArray docsArray = responseObject.getJSONArray("docs");
    List<Pair<String, List<Pair<String, Double>>>> ans = new ArrayList<>();
    for (Iterator<Object> it = docsArray.iterator(); it.hasNext(); ) {
      JSONObject doc = (JSONObject) it.next();
      if ((boolean) doc.get("found")) {
        JSONObject termsObject =
            doc.getJSONObject("term_vectors").getJSONObject("text").getJSONObject("terms");
        double maxScore = 0;
        ArrayList<Pair<String, Double>> tempKeywords = new ArrayList<>();
        for (String key : termsObject.keySet()) {
          double score = (double) termsObject.getJSONObject(key).get("score");
          if (score > maxScore) {
            maxScore = score;
          }
          tempKeywords.add(new Pair<>(key, score));
        }
        ArrayList<Pair<String, Double>> keywords = new ArrayList<>();
        for (Pair<String, Double> temp : tempKeywords) {
          keywords.add(new Pair<>(temp.getKey(), temp.getValue() / maxScore));
        }
        ans.add(new Pair<>((String) doc.get("_id"), keywords));
      }
    }
    return ans;
  }

  public void closeClient() throws IOException {
    client.close();
  }
}
