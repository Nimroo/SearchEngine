package ir.sahab.nimroo.elasticsearch;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ElasticAnalysisClient {
  private RestClient client;

  public ElasticAnalysisClient() {
    client =
        RestClient.builder(new HttpHost("46.4.99.108", 9200, "http"))
            .setRequestConfigCallback(
                requestConfigBuilder ->
                    requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(60000))
            .setMaxRetryTimeoutMillis(60000)
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
    for (HashMap.Entry<String, Double> temp : ans.entrySet()) {
      System.out.println(temp.getKey() + "=" + temp.getValue());
    }
    return ans;
  }

  public void getInterestingKeywordsForMultiDocuments(){

  }

  public void closeClient() throws IOException {
    client.close();
  }
}
