package ir.sahab.nimroo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {

  private Config() {}

  public static String server1Address;
  public static String server2Address;
  public static String server3Address;
  public static int kafka1Port;
  public static int kafka2Port;
  public static int kafka3Port;
  public static int kafkaProducerBatchSize;
  public static int kafkaProducerLingerMS;
  public static String kafkaProducerAcks;
  public static String kafkaConsumerGroupId;
  public static String kafkaConsumerMaxPollRecords;
  public static String kafkaConsumerSessionTimeoutsMS;
  public static String kafkaLinkTopicName;
  public static String kafkaHtmlTopicName;
  public static int httpRequestMaxConnection;
  public static int httpRequestMaxConnectionPerHost;
  public static int httpRequestTimeout = 15000;
  public static int httpSocketTimeout = 5000;
  public static String elasticsearchIndexName;
  public static int linkPartition;
  public static String hadoopCoreSite;
  public static String hBaseSite;
  public static int shufflerQueueSize;
  public static int shuffleSize;

  public static void load() {
    String appConfigPath = "app.properties";
    Properties properties = new Properties();

    try {
      properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(appConfigPath));
      Config.server1Address = properties.getProperty("server1.ip");
      Config.server2Address = properties.getProperty("server2.ip");
      Config.server3Address = properties.getProperty("server3.ip");

      Config.kafka1Port = Integer.parseInt(properties.getProperty("kafka1.port"));
      Config.kafka2Port = Integer.parseInt(properties.getProperty("kafka2.port"));
      Config.kafka3Port = Integer.parseInt(properties.getProperty("kafka3.port"));
      Config.kafkaProducerBatchSize =
          Integer.parseInt(properties.getProperty("kafka.producer.batch.size"));
      Config.kafkaProducerLingerMS =
          Integer.parseInt(properties.getProperty("kafka.producer.linger.ms"));
      Config.kafkaProducerAcks = properties.getProperty("kafka.producer.acks");
      Config.kafkaConsumerGroupId = properties.getProperty("kafka.consumer.groupId");
      Config.kafkaConsumerMaxPollRecords =
          properties.getProperty("kafka.consumer.max.poll.records");
      Config.kafkaConsumerSessionTimeoutsMS =
          properties.getProperty("kafka.consumer.session.timeout.ms");
      Config.kafkaLinkTopicName = properties.getProperty("kafka.consumer.linkTopic");
      Config.kafkaHtmlTopicName = properties.getProperty("kafka.consumer.htmlTopic");
      Config.httpRequestMaxConnection =
          Integer.parseInt(properties.getProperty("http.client.maxConnection"));
      Config.httpRequestMaxConnectionPerHost =
          Integer.parseInt(properties.getProperty("http.client.perHost"));
      Config.elasticsearchIndexName = properties.getProperty("elasticsearch.index.name");
      Config.linkPartition = Integer.parseInt(properties.getProperty("kafka.consumer.htmlTopic.partition"));
      Config.hadoopCoreSite = properties.getProperty("core.site.path");
      Config.hBaseSite = properties.getProperty("hbase.site.path");
      Config.shuffleSize = Integer.parseInt(properties.getProperty("shuffler.shuffle.size"));
      Config.shufflerQueueSize = Integer.parseInt(properties.getProperty("shuffler.queue.size"));

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
