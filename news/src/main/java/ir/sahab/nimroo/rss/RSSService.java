package ir.sahab.nimroo.rss;

import ir.sahab.nimroo.elasticsearch.ElasticClient;
import ir.sahab.nimroo.hbase.NewsRepository;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.xml.sax.SAXException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class RSSService {
  private Logger logger;
  private ExecutorService executorService;
  private ElasticClient elasticClient;

  RSSService() {
    elasticClient = new ElasticClient();
    PropertyConfigurator.configure(
        RSSService.class.getClassLoader().getResource("log4j.properties"));
    logger = Logger.getLogger(RSSService.class);
    executorService =
        new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(50));
  }

  private void updateNews() throws IOException {
    while (true) {
      ResultScanner scanner = null;
      scanner = NewsRepository.getInstance().getResultScanner("newsAgency");
      logger.info("update News run just now.");
      for (Result result = scanner.next(); (result != null); result = scanner.next()) {
        Result finalResult = result;
        executorService.submit(
            () -> {
              crawlRSS(finalResult);
            });
      }
      try {
        TimeUnit.MINUTES.sleep(10);
      } catch (InterruptedException e) {
        logger.warn("concurrent problem in RSS Controller!\nthread don't want to sleep!!", e);
      }
    }
  }

  private void crawlRSS(Result result) {
    String rssUrl =
        Bytes.toString(result.getValue(Bytes.toBytes("newsAgency"), Bytes.toBytes("url")));
    String configStr =
        Bytes.toString(result.getValue(Bytes.toBytes("newsAgency"), Bytes.toBytes("config")));
    String last =
        Bytes.toString(result.getValue(Bytes.toBytes("newsAgency"), Bytes.toBytes("last")));

    if(rssUrl == null || configStr == null)
      return;
    if(last == null)
      last = "";
    ArrayList<HashMap<String, String>> rssData = parsRSS(getRSSDocument(rssUrl));
    ArrayList<String> config = new ArrayList<>(Arrays.asList(configStr.split("/")));
    if (!rssData.isEmpty()) {
      try {
        NewsRepository.getInstance()
            .putToTable(
                "newsAgency",
                Bytes.toBytes("last"),
                Bytes.toBytes(DigestUtils.md5Hex(rssUrl)),
                Bytes.toBytes(rssData.get(0).get("link")));
      } catch (IOException e) {
        logger.warn(e);
      }
    }
    for (HashMap<String, String> hashMap : rssData) {
      if (last.equals(hashMap.get("link"))) break;
      String link = hashMap.get("link");
      String title = hashMap.get("title");
      String text = crawlNews(hashMap.get("link"), config);
      String pubDate = hashMap.get("pubDate");
      try {
        elasticClient.addNewsToBulkOfElastic(link, title, text, pubDate, DigestUtils.md5Hex(link), RssConfig.newsIndexNameForElastic);
      } catch (IOException e) {
        logger.error("can not add news to elastic", e);
      }
    }
    try {
      elasticClient.addBulkToElastic();
    } catch (IOException e) {
      logger.error("add Bulk to Elastic throw IO Exception." ,e);
    }
  }

  ArrayList<HashMap<String, String>> parsRSS(org.w3c.dom.Document domTree) {
    ArrayList<HashMap<String, String>> rssDataMap = new ArrayList<>();
    for (int i = 0; i < domTree.getElementsByTagName("item").getLength(); i++) {
      rssDataMap.add(new HashMap<>());
      for (int j = 0;
          j < domTree.getElementsByTagName("item").item(i).getChildNodes().getLength();
          j++) {
        if (checkTag(domTree, i, j, "title")) {
          rssDataMap.get(i).put("title", contentOfNode(domTree, i, j));
        } else if (checkTag(domTree, i, j, "link")) {
          rssDataMap.get(i).put("link", contentOfNode(domTree, i, j));
        } else if (checkTag(domTree, i, j, "pubDate")) {
          rssDataMap.get(i).put("pubDate", contentOfNode(domTree, i, j));
        }
      }
    }
    return rssDataMap;
  }

  private boolean checkTag(org.w3c.dom.Document domTree, int domNodeNumber, int itemNodeNumber, String tag) {
    return domTree
        .getElementsByTagName("item")
        .item(domNodeNumber)
        .getChildNodes()
        .item(itemNodeNumber)
        .toString()
        .contains(tag);
  }

  private String contentOfNode(org.w3c.dom.Document domTree, int domNodeNumber, int itemNodeNumber) {
    return domTree
        .getElementsByTagName("item")
        .item(domNodeNumber)
        .getChildNodes()
        .item(itemNodeNumber)
        .getTextContent();
  }

  private String crawlNews(String link, ArrayList<String> siteConfig) {
    String body;
    try {
      Document doc = Jsoup.connect(link).timeout(15000).get();
      Elements rows = doc.getElementsByAttributeValue(siteConfig.get(0), siteConfig.get(1));
      body = rows.first().text();
    } catch (IOException
        | NullPointerException
        | ExceptionInInitializerError
        | IndexOutOfBoundsException e) {
      logger.warn("not found body for this link." + link, e);
      body = "not Found!";
    }
    return body;
  }

  private org.w3c.dom.Document getRSSDocument(String rssUrl) {
    DocumentBuilderFactory domBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder domBuilder = null;
    URL url = null;
    try {
      domBuilder = domBuilderFactory.newDocumentBuilder();
      url = new URL(rssUrl);
      URLConnection con = url.openConnection();
      con.setConnectTimeout(15000);
      con.setReadTimeout(15000);
      con.setRequestProperty("User-Agent","Mozilla/5.0 ( compatible ) ");
      return domBuilder.parse(con.getInputStream());
    } catch (SAXException | IOException | ParserConfigurationException e) {
      logger.error(e);
    }
    return null;
  }

  public void runNewsUpdater() {
    executorService.submit(
        () -> {
          try {
            elasticClient.createIndexForNews(RssConfig.newsIndexNameForElastic);
            updateNews();
          } catch (IOException e) {
            logger.error(e);
          }
        });
  }
}
