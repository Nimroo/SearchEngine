package ir.sahab.nimroo;

import ir.sahab.nimroo.elasticsearch.ElasticClient;
//import ir.sahab.nimroo.keywordextraction.ElasticAnalysisClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class ElasticsearchUI {
  private Scanner scanner;
  private ElasticClient elasticClient;
  private static final Logger LOGGER = Logger.getLogger(ElasticsearchUI.class);

  public static void main(String[] args) {
    Config.load();
    ElasticsearchUI elasticsearchUI = new ElasticsearchUI();
    try {
      elasticsearchUI.test();
    } catch (URISyntaxException | IOException e) {
      LOGGER.error(e);
    }
  }

  public void test() throws URISyntaxException, IOException {/*
    //elasticClient = new ElasticClient();
    //elasticClient.readObsceneWordsForSearch();
    scanner = new Scanner(System.in);
    //elasticClient.createIndexForNews("lasttest22");
    //elasticClient.addBulkToElastic();
    ElasticAnalysisClient elasticAnalysisClient = new ElasticAnalysisClient();
    ArrayList<String> temp = new ArrayList<>();
    temp.add("f66a3effb565a76c8f9a47e9c0229fe3");
    temp.add("0055b3b0c1a69eb9ecedd7172d9ca0ad");
    temp.add("005cb0833dd4c0770059f7e707d77ccf");
    temp.add("0064a936e27c125a6c7551a3479b27d2");
    temp.add("00672f3c758006985faff360db514e29");
    temp.add("00672c273b6b14c0f250c02e9e2b3915");
    temp.add("006734854d82eb50944db21487f58826");
    temp.add("0066dbf7a4cdcf02d3ff72d24f2d5955");
    temp.add("0066e0379344b80f22bf0f423cf4cc39");
    temp.add("0066d40b34a21b1928f0c76487d036d8");
    temp.add("f8a66e988f38b782c68d422a0d5c8f3e");
    elasticAnalysisClient.getInterestingKeywordsForMultiDocuments("webpage",temp,5);
    search();
    System.out.println("here");*/
  }
 /*
  public static void main(String[] args) {
    Config.load();

    ElasticsearchUI elasticsearchUI = new ElasticsearchUI();
    try {
      elasticsearchUI.start();
    } catch (URISyntaxException e) {
      LOGGER.error(e);
    }
  }
*/
  public void start() throws URISyntaxException {
    elasticClient = new ElasticClient(Config.server1Address);
    elasticClient.readObsceneWordsForSearch();
    scanner = new Scanner(System.in);
    while (true) {
      if (elasticClient.getSafeSearch()) {
        System.out.println(
            "write \"search\" to start search.\n"
                + "write \"advancedSearch\" to start advancedSearch.\n"
                + "write \"safeOff\" to turn off safe search.\n");
      } else {
        System.out.println(
            "write \"search\" to start search.\n"
                + "write \"advancedSearch\" to start advancedSearch.\n"
                + "write \"safeOn\" to turn on safe search.\n");
      }
      String input = scanner.next().toLowerCase();
      switch (input) {
        case "advancedsearch":
          try {
            advancedSearch();
          } catch (IOException e) {
            e.printStackTrace();
          }
          break;
        case "search":
          try {
            search();
          } catch (IOException e) {
            e.printStackTrace();
          }
          break;
        case "safeon":
          elasticClient.setSafeSearch(true);
          break;
        case "safeoff":
          elasticClient.setSafeSearch(false);
          break;
        default:
          System.out.println("input is not valid.\nplease try again.\n");
          break;
      }
    }
  }

  private void search() throws IOException {
    System.out.println("Enter your search text:\n");
    //scanner.nextLine();
    String searchText = scanner.nextLine();
    HashMap<String, Double> ans =
        elasticClient.simpleSearchInElasticForWebPage(
            searchText, "lasttest1", false);
    for (HashMap.Entry<String, Double> temp : ans.entrySet()) {
      System.out.println(temp.getKey() + "     " + temp.getValue());
    }
  }

  private void advancedSearch() throws IOException {
    ArrayList<String> must = new ArrayList<>();
    ArrayList<String> mustNot = new ArrayList<>();
    ArrayList<String> should = new ArrayList<>();
    while (true) {
      System.out.println(
          "Write \"must\" to add a phrase you absolutely want it to be in the page.\n"
              + "write \"mustnot\" to add a phrase you don't want to see in the page.\n"
              + "write \"should\" to add a phrase you prefer to see in the page.\n"
              + "write \"done\" to get 10 best result.\n");
      String input = scanner.next().toLowerCase();
      scanner.nextLine();
      switch (input) {
        case "must":
          System.out.println("Enter your phrase:\n");
          must.add(scanner.nextLine());
          break;
        case "mustnot":
          System.out.println("Enter your phrase:\n");
          mustNot.add(scanner.nextLine());
          break;
        case "should":
          System.out.println("Enter your phrase:\n");
          should.add(scanner.nextLine());
          break;
        case "done":
          HashMap<String, Double> ans =
              elasticClient.advancedSearchInElasticForWebPage(
                  must, mustNot, should, Config.elasticsearchIndexName, true);
          for (HashMap.Entry<String, Double> temp : ans.entrySet()) {
            System.out.println(temp.getKey() + "     " + temp.getValue());
          }
          return;
        default:
          System.out.println("input is not valid.\nplease try again.\n");
          break;
      }
    }
  }
}
