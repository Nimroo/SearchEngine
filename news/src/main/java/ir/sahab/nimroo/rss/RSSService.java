package ir.sahab.nimroo.rss;

import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.elasticsearch.ElasticClient;
import ir.sahab.nimroo.hbase.NewsRepository;
import ir.sahab.nimroo.keywordextraction.ElasticAnalysisClient;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
  private ElasticAnalysisClient elasticAnalysisClient;
  private Map<String, Double> trendWords;
  private ArrayList<String> top5Trends;

  RSSService() {
    trendWords = new HashMap<>();
    top5Trends = new ArrayList<>();
    elasticAnalysisClient = new ElasticAnalysisClient(Config.server3Address);
    elasticClient = new ElasticClient(Config.server1Address);
    PropertyConfigurator.configure(
        RSSService.class.getClassLoader().getResource("log4j.properties"));
    logger = Logger.getLogger(RSSService.class);
    executorService =
        new ThreadPoolExecutor(40, 40, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(150));
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
        logger.info("I will goto sleep for 10 minute.");
        TimeUnit.MINUTES.sleep(10);
      } catch (InterruptedException e) {
        logger.warn("concurrent problem in RSS Controller!\nthread don't want to sleep!!", e);
      }
      try {
        elasticClient.addBulkToElastic();
        logger.info("news added to elastic.");
      } catch (IOException e) {
        logger.error("add Bulk to Elastic throw IO Exception.", e);
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

    if (rssUrl == null || configStr == null) return;
    if (last == null) last = "";
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
      String pubDate =
          convertToLocalDateTimeViaMilisecond(reFormatPublishDate(hashMap.get("pubDate")))
              .toString();
      if (text.equals("not Found!")) continue;
      try {
        elasticClient.addNewsToBulkOfElastic(
            link,
            title,
            text,
            pubDate,
            DigestUtils.md5Hex(link),
            RssConfig.newsIndexNameForElastic);
        executorService.submit(
            () -> {
              updateTrendWordsValue(DigestUtils.md5Hex(link));
            });
        logger.info("correct news add to  bulk of elastic.");
      } catch (IOException e) {
        logger.error("can not add news to elastic", e);
      }
    }
  }

  private void updateTrendWordsValue(String id) {
    HashMap<String, Double> top5;
    try {
      top5 = elasticAnalysisClient.getInterestingKeywords(RssConfig.newsIndexNameForElastic, id, 5);
    } catch (IOException e) {
      logger.warn(e);
      return;
    }
    top5.forEach(
        (k, v) -> {
          if (!RssConfig.stopWords.contains(k)) {
            try {
              NewsRepository.getInstance()
                  .putToTable("trendWords", Bytes.toBytes(k), Bytes.toBytes(id), Bytes.toBytes(v));
            } catch (IOException e) {
              logger.warn(e);
            }
          }
        });
  }

  private Date reFormatPublishDate(String pubDate) {
    ArrayList<SimpleDateFormat> formats = new ArrayList<>();
    formats.add(new SimpleDateFormat("EEE, dd MMM yyyy hh:mm Z"));
    formats.add(new SimpleDateFormat("dd MMM yyyy hh:mm:ss Z"));
    formats.add(new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss Z"));
    formats.add(new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss Z"));
    Date date = null;
    if (pubDate == null) {
      pubDate = (new Date()).toString();
    }
    for (SimpleDateFormat formatter : formats) {
      try {
        date = formatter.parse(pubDate);
        break;
      } catch (ParseException e) {
        date = new Date();
      }
    }
    return date;
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

  private boolean checkTag(
      org.w3c.dom.Document domTree, int domNodeNumber, int itemNodeNumber, String tag) {
    return domTree
        .getElementsByTagName("item")
        .item(domNodeNumber)
        .getChildNodes()
        .item(itemNodeNumber)
        .toString()
        .contains(tag);
  }

  private String contentOfNode(
      org.w3c.dom.Document domTree, int domNodeNumber, int itemNodeNumber) {
    return domTree
        .getElementsByTagName("item")
        .item(domNodeNumber)
        .getChildNodes()
        .item(itemNodeNumber)
        .getTextContent();
  }

  private String crawlNews(String link, ArrayList<String> siteConfig) {
    String body = "not Found!";
    try {
      Document doc = Jsoup.connect(link).timeout(15000).get();
      Elements rows = doc.getElementsByAttributeValue(siteConfig.get(0), siteConfig.get(1));
      body = rows.first().text();
    } catch (IOException
        | NullPointerException
        | ExceptionInInitializerError
        | IndexOutOfBoundsException e) {
      logger.warn("not found body for this link." + link);
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
      con.setRequestProperty("User-Agent", "Mozilla/5.0 ( compatible ) ");
      return domBuilder.parse(con.getInputStream());
    } catch (SAXException | IOException | ParserConfigurationException e) {
      logger.error(e);
    }
    return null;
  }

  public LocalDateTime convertToLocalDateTimeViaMilisecond(Date dateToConvert) {
    return Instant.ofEpochMilli(dateToConvert.getTime())
        .atZone(ZoneId.systemDefault())
        .toLocalDateTime();
  }

  private void updateTrendWords() throws IOException {
    while (true) {
      trendWords.clear();
      top5Trends.clear();
      ResultScanner scanner = null;
      try {
        scanner =
            NewsRepository.getInstance()
                .getResultScannerWithTimeRange(
                    "trendWords",
                    System.currentTimeMillis() - (18 * 60 * 60 * 1000),
                    System.currentTimeMillis());
      } catch (IOException e) {
        logger.error(e);
      }
      if (scanner == null) {
        logger.error("error in RSSService Class, scanner is null.");
        continue;
      }
      for (Result result = scanner.next(); (result != null); result = scanner.next()) {
        for (Cell cell : result.listCells()) {
          String word = Bytes.toString(CellUtil.cloneQualifier(cell));
          Double value = Bytes.toDouble(CellUtil.cloneValue(cell));
          if (!trendWords.containsKey(word)) trendWords.put(word, value);
          else trendWords.replace(word, trendWords.get(word) + value);
        }
      }
      for (int i = 0; i < Math.min(10, trendWords.size()); i++) {
        Map.Entry<String, Double> maxEntry = null;
        for (Map.Entry<String, Double> entry : trendWords.entrySet()) {
          if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0.0000) {
            maxEntry = entry;
          }
        }
        if (maxEntry == null) continue;
        trendWords.remove(maxEntry.getKey());
        top5Trends.add(maxEntry.getKey());
        NewsRepository.getInstance().putToTable("trendWords", Bytes.toBytes(i), Bytes.toBytes("top10"), Bytes.toBytes(maxEntry.getKey()));
        logger.info("one of trend words; " + maxEntry.getKey() + " " + maxEntry.getValue());
      }
      try {
        TimeUnit.HOURS.sleep(2);
      } catch (InterruptedException e) {
        logger.warn(e);
      }
    }
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
    executorService.submit(
        () -> {
          try {
            updateTrendWords();
          } catch (IOException e) {
            logger.error(e);
          }
        });
  }
}
