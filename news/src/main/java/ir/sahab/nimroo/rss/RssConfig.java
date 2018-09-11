package ir.sahab.nimroo.rss;

import ir.sahab.nimroo.hbase.NewsRepository;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Scanner;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class RssConfig {

  private RssConfig() {}

  public static int numberOfRssSite;
  public static String newsIndexNameForElastic;
  public static HashSet<String> stopWords;

  public static void load() {

    Logger logger = Logger.getLogger(RssConfig.class);
    String appConfigPath = "rss.properties";
    Properties properties = new Properties();
    stopWords = new HashSet<>();

    try {
      properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(appConfigPath));
      RssConfig.numberOfRssSite = Integer.parseInt(properties.getProperty("numberOfRssSite"));
      RssConfig.newsIndexNameForElastic = properties.getProperty("newsIndexNameForElastic");
      for(int i=0; i<RssConfig.numberOfRssSite; i++){
        String tmpRssUrl = properties.getProperty("Site."+i+".RssUrl");
        String tmpConfig = properties.getProperty("Site."+i+".Config");
        NewsRepository.getInstance().putToTable("newsAgency", Bytes.toBytes("url"), Bytes.toBytes(DigestUtils.md5Hex(tmpRssUrl)), Bytes.toBytes(tmpRssUrl));
        NewsRepository.getInstance().putToTable("newsAgency", Bytes.toBytes("config"), Bytes.toBytes(DigestUtils.md5Hex(tmpRssUrl)), Bytes.toBytes(tmpConfig));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    File file = new File("news/src/main/resources/stopWords.txt");
    Scanner sc = null;
    try {
      sc = new Scanner(file);
    } catch (FileNotFoundException e) {
      logger.error(e);
    }
    while (sc.hasNextLine()) {
      stopWords.add(sc.nextLine().toLowerCase());
    }
  }

}
