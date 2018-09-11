package ir.sahab.nimroo.elasticsearch;

import ir.sahab.nimroo.hbase.CrawlerRepository;
import javafx.util.Pair;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

public class SearchUIConnector {
  private ElasticClient elasticClient;

  public SearchUIConnector() throws URISyntaxException {
    elasticClient = new ElasticClient("46.4.99.108");
    elasticClient.readObsceneWordsForSearch();
  }

  public ArrayList<String> simpleSearch(
      String searchText, String index, boolean safety, boolean pageRank) throws IOException {
    elasticClient.setSafeSearch(safety);
    HashMap<String, Double> ans =
        elasticClient.simpleSearchInElasticForWebPage(searchText, index, pageRank);
    return makeArrayList(ans, pageRank);
  }

  public ArrayList<String> advancedSearch(
      ArrayList<String> mustFind,
      ArrayList<String> mustNotFind,
      ArrayList<String> shouldFind,
      String index,
      boolean safety,
      boolean pageRank)
      throws IOException {
    elasticClient.setSafeSearch(safety);
    HashMap<String, Double> ans =
        elasticClient.advancedSearchInElasticForWebPage(
            mustFind, mustNotFind, shouldFind, index, pageRank);
    return makeArrayList(ans, pageRank);
  }

  public ArrayList<Pair<String, String>> newsSearch(String searchText, String index)
      throws IOException {
    HashMap<String, Pair<String, Double>> ans =
        elasticClient.searchInElasticForNews(searchText, index);
    ArrayList<Pair<String, String>> answer = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      double maxFinalScore = 0;
      String linkOfMaxScore = null;
      for (HashMap.Entry<String, Pair<String, Double>> temp : ans.entrySet()) {
        if (maxFinalScore < temp.getValue().getValue()) {
          maxFinalScore = temp.getValue().getValue();
          linkOfMaxScore = temp.getKey();
        }
      }
      answer.add(new Pair<>(linkOfMaxScore, ans.get(linkOfMaxScore).getKey()));
      ans.remove(linkOfMaxScore);
    }
    return answer;
  }

  private ArrayList<String> makeArrayList(HashMap<String, Double> links, boolean pageRank) {
    if (!pageRank) {
      ArrayList<String> answer = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        double maxFinalScore = 0;
        String linkOfMaxScore = null;
        for (HashMap.Entry<String, Double> temp : links.entrySet()) {
          if (maxFinalScore < temp.getValue()) {
            maxFinalScore = temp.getValue();
            linkOfMaxScore = temp.getKey();
          }
        }
        answer.add(linkOfMaxScore);
        links.remove(linkOfMaxScore);
      }
      return answer;
    } else {
      HashMap<String, Double> mapForScoring = new HashMap<>();
      ArrayList<String> answer = new ArrayList<>();
      for (HashMap.Entry<String, Double> temp : links.entrySet()) {
        double pageRangTemp = CrawlerRepository.getInstance().getPageRank(temp.getKey());
        if (pageRangTemp == 1d) {
          pageRangTemp = 0d;
        }
        mapForScoring.put(temp.getKey(), pageRangTemp * temp.getValue());
      }
      for (int i = 0; i < 10; i++) {
        double maxFinalScore = 0;
        String linkOfMaxScore = null;
        for (HashMap.Entry<String, Double> temp : mapForScoring.entrySet()) {
          if (maxFinalScore < temp.getValue()) {
            maxFinalScore = temp.getValue();
            linkOfMaxScore = temp.getKey();
          }
        }
        if (linkOfMaxScore != null) {
          answer.add(linkOfMaxScore);
          mapForScoring.remove(linkOfMaxScore);
        }
      }
      return answer;
    }
  }
}
