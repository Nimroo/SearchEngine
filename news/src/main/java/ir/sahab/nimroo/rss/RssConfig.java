package ir.sahab.nimroo.rss;

import ir.sahab.nimroo.hbase.NewsRepository;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class RssConfig {

  private RssConfig() {}

  public static int numberOfRssSite;
  public static String newsIndexNameForElastic;

  public static void load() {
    String appConfigPath = "rss.properties";
    Properties properties = new Properties();

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
  }

}
