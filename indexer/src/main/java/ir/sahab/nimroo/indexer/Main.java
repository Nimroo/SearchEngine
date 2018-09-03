package ir.sahab.nimroo.indexer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.elasticsearch.ElasticClient;
import ir.sahab.nimroo.hbase.CrawlerRepository;
import ir.sahab.nimroo.kafka.KafkaHtmlConsumer;
import ir.sahab.nimroo.model.Link;
import ir.sahab.nimroo.model.PageData;
import ir.sahab.nimroo.serialization.PageDataSerializer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class Main {

  private static Logger logger;

  ElasticClient elasticClient;
  KafkaHtmlConsumer kafkaHtmlConsumer;
  int numberOfStoreDocument = 0;
  private final MetricRegistry crawlerMetrics = new MetricRegistry();
  private final Meter persistRate = crawlerMetrics.meter(MetricRegistry.name("persist", "rate"));

  private Main() {
    PropertyConfigurator.configure(Main.class.getClassLoader().getResource("log4j.properties"));
    logger = Logger.getLogger(Main.class);
    elasticClient = new ElasticClient();
    kafkaHtmlConsumer = new KafkaHtmlConsumer();

    Graphite graphite = new Graphite(new InetSocketAddress(Config.server1Address, 2003));
    GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(crawlerMetrics)
            .prefixedWith("omlet")
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .build(graphite);
    graphiteReporter.start(10, TimeUnit.SECONDS);
  }

  private void storeFromKafka() {
    ArrayList<byte[]> pageDatas = kafkaHtmlConsumer.get();
    ArrayList<ArrayList<Put>> putArray = new ArrayList<>();
    for (byte[] bytes : pageDatas) {
      PageData pageData;
      try {
        pageData = PageDataSerializer.getInstance().deserialize(bytes);
      } catch (com.github.os72.protobuf351.InvalidProtocolBufferException e) {
        logger.error(e);
        continue;
      }
      addToElasticBulk(pageData);
      putArray.add(addToHBaseBulk(pageData));
    }
    try {
      elasticClient.addBulkToElastic();
    } catch (IOException e) {
      logger.error("error occur in storeFromKafka method.", e);
    }
    for (ArrayList list : putArray){
      try {
        CrawlerRepository.getInstance().putToTable(list);
      } catch (IOException e) {
        logger.error("error occur in storeFromKafka method.", e);
      }
    }
    persistRate.mark(pageDatas.size());
    logger.info(numberOfStoreDocument + " store from kafka to HBase and Elastic.");
  }

  private ArrayList<Put> addToHBaseBulk(PageData pageData) {
    ArrayList<Put> arrayList = new ArrayList<>();
    Put p = new Put(toBytes(DigestUtils.md5Hex(pageData.getUrl())));
    p.addColumn(toBytes("outLink"), toBytes("url"), toBytes(pageData.getUrl()));
    arrayList.add(p);
    for (Link link : pageData.getLinks()) {
      Put tmp = new Put(toBytes(DigestUtils.md5Hex(pageData.getUrl())));
      tmp.addColumn(toBytes("outLink"), toBytes(link.getLink()), toBytes(link.getAnchor()));
      arrayList.add(tmp);
    }
    return arrayList;
  }

  private void addToElasticBulk(PageData pageData) {
    try {
      elasticClient.addWebPageToBulkOfElastic(pageData, DigestUtils.md5Hex(pageData.getUrl()), Config.elasticsearchIndexName);
    } catch (IOException e) {
      logger.error(e);
    }
  }

  public static void main(String[] args) {
    Config.load();
    Main main = new Main();
    if (args.length != 1) {
      System.err.println(
          "print enter exactly one argument.\n enter 'store' --> for storing data to HBase and elastic.");
      System.exit(1);
    }
    if (args[0].equals("store")) {
      main.storeFromKafka();
    }
  }
}
