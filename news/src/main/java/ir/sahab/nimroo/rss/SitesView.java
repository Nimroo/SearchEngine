package ir.sahab.nimroo.rss;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Scanner;

public class SitesView {
  private RssNews rssNews;
  private ArrayList<String> stopWords;

  public SitesView() throws FileNotFoundException, UnsupportedEncodingException, URISyntaxException {
    rssNews = new RssNews();

    File file = new File(getClass().getClassLoader().getResource("rss links").toURI());
    Scanner sc = new Scanner(file);
    while (sc.hasNextLine()) {
      rssNews.addSite(sc.nextLine(), "no name");
    }
    stopWords = new ArrayList<>();
    file = new File(getClass().getClassLoader().getResource("stopWords").toURI());
    sc = new Scanner(file);
    while (sc.hasNextLine()) {
      stopWords.add(sc.nextLine());
    }
  }

  public ArrayList<String> getKeywordsOfLatestNews() {
    ArrayList<String> topics = rssNews.getNewsTitle();
    KeywordExtractor keywordExtractor = new KeywordExtractor(stopWords, 20);
    ArrayList<String> regex = new ArrayList<>();
    regex.add(":");
    regex.add("'s");
    regex.add("’s");
    regex.add("‘");
    regex.add("'");
    regex.add("’");
    regex.add(",");
    regex.add("&");
    regex.add("_");
    regex.add("–");
    regex.add("0");
    regex.add("1");
    regex.add("2");
    regex.add("3");
    regex.add("4");
    regex.add("5");
    regex.add("6");
    regex.add("7");
    regex.add("8");
    regex.add("9");
    for (String topic : topics) {
      String temp = topic.toLowerCase();
      for (String aRegex : regex) {
        temp = temp.replaceAll(aRegex, "");
      }
      keywordExtractor.addForExtractingKeywords(temp);
    }
    return keywordExtractor.getKeywords();
  }
}
