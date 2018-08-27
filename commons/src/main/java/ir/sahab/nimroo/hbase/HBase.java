//package ir.sahab.nimroo.hbase;
//
//import ir.sahab.nimroo.Config;
//import ir.sahab.nimroo.model.PageData;
//import ir.sahab.nimroo.kafka.KafkaHtmlConsumer;
//import ir.sahab.nimroo.serialization.LinkArraySerializer;
//import ir.sahab.nimroo.serialization.PageDataSerializer;
//import org.apache.hadoop.fs.Path;
//import org.apache.commons.codec.digest.DigestUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.log4j.Logger;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.*;
//
//import static org.apache.hadoop.hbase.util.Bytes.toBytes;
//
//
//public class HBase {
//
//  private static HBase ourInstance;
//
//  public static HBase getInstance() {
//      if (ourInstance == null) {
//          synchronized (HBase.class) {
//              if (ourInstance == null) {
//                  try {
//                      ourInstance = new HBase();
//                  }
//                  catch (Exception e) {
//                      e.printStackTrace();
//                  }
//              }
//          }
//      }
//      return ourInstance;
//  }
//
//  private static final Logger logger = Logger.getLogger(HBase.class);
//  private Configuration config;
//  private String linksFamily;
//  private String pageDataFamily;
//  private String pageRankFamily;
//  private String tableName;
//  private KafkaHtmlConsumer kafkaHtmlConsumer;
//  private ExecutorService executorService;
//  private Connection defConn;
//  private Table defTable;
//  private int counter = 0;
//  private int total = 0;
//  private List<Put> batch;
//  static long firstStartTime;
//
//  private HBase() {
//    firstStartTime = System.currentTimeMillis();
//    executorService =
//        new ThreadPoolExecutor(15, 15, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1000));
//    config = HBaseConfiguration.create();
//    config.addResource(new Path(Config.hBaseSite));
//    config.addResource(new Path(Config.hBaseCoreSite));
//    logger.info("hbase configs are loaded !!!");
//    tableName = "nimroo";
//    linksFamily = "links";
//    pageDataFamily = "pageData";
//    pageRankFamily = "pageRank";
//
//    try {
//      createTable();
//    } catch (IOException e) {
//      logger.error("possibly we can not get admin from HBase ir.sahab.nimroo.connection!", e);
//    }
//
//    try {
//      defConn = ConnectionFactory.createConnection(config);
//      defTable = defConn.getTable(TableName.valueOf(tableName));
//    } catch (IOException e) {
//      logger.error("can not get ir.sahab.nimroo.connection from HBase!", e);
//      System.exit(499);
//    }
//  }
//
//  private Admin getAdmin() {
//    try {
//      return ConnectionFactory.createConnection(config).getAdmin();
//    } catch (IOException e) {
//      return null;
//    }
//  }
//
//  public void createTable() throws IOException {
//    Admin admin = getAdmin();
//    if (admin == null) return;
//    if (admin.tableExists(TableName.valueOf(tableName))) {
//      return;
//    }
//    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
//    tableDescriptor.addFamily(new HColumnDescriptor(linksFamily));
//    tableDescriptor.addFamily(new HColumnDescriptor(pageDataFamily));
//    tableDescriptor.addFamily(new HColumnDescriptor(pageRankFamily));
//
//    byte[][] regions =
//        new byte[][] {
//          toBytes("0"),
//          toBytes("1"),
//          toBytes("2"),
//          toBytes("3"),
//          toBytes("4"),
//          toBytes("5"),
//          toBytes("6"),
//          toBytes("7"),
//          toBytes("8"),
//          toBytes("9"),
//          toBytes("a"),
//          toBytes("b"),
//          toBytes("c"),
//          toBytes("d"),
//          toBytes("e"),
//          toBytes("f")
//        };
//
//    admin.createTable(tableDescriptor, regions);
//  }
//
//  public void dropTable(String tableName) throws IOException {
//    Admin admin = getAdmin();
//    if (admin == null) return;
//    if (!admin.tableExists(TableName.valueOf(tableName))) {
//      return;
//    }
//    admin.disableTable(TableName.valueOf(tableName));
//    admin.deleteTable(TableName.valueOf(tableName));
//  }
//
////  public boolean isDuplicateUrl(String link) {
////    logger.trace("isDuplicateUrl is called !!!");
////    return isUrlExist(link, defTable);
////  }
////
////  private boolean isUrlExist(String link, Table table) {
////    try {
////      Get get =
////          new Get(toBytes(DigestUtils.md5Hex(link)))
////              .addColumn(toBytes(linksFamily), toBytes("link"));
////      if (!table.exists(get)) {
////        Put p = new Put(toBytes(DigestUtils.md5Hex(link)));
////        p.addColumn(toBytes(linksFamily), toBytes("link"), toBytes(0));
////        table.put(p);
////        return false;
////      }
////      return true;
////    } catch (IOException e) {
////      logger.warn("some exception happen in isUrlExist method!" + e);
////      return false;
////    }
////  }
//
//  @Deprecated
//  public void storeFromKafka() throws InterruptedException {
//    Table table;
//    try {
//      Connection connection = ConnectionFactory.createConnection(config);
//      table = connection.getTable(TableName.valueOf(tableName));
//    } catch (IOException e) {
//      logger.error("can not get ir.sahab.nimroo.connection from HBase!", e);
//      return;
//    }
//
//    while (true) {
//      long startTime = System.currentTimeMillis();
//      long startTimeKafka = System.currentTimeMillis();
//      ArrayList<byte[]> pageDatas = kafkaHtmlConsumer.get();
//      long finishTimeKafka = System.currentTimeMillis();
//      logger.info("get from kafka = " + pageDatas.size());
//      for (byte[] bytes : pageDatas) {
//        PageData pageData = null;
//        try {
//          pageData = PageDataSerializer.getInstance().deserialize(bytes);
//        } catch (com.github.os72.protobuf351.InvalidProtocolBufferException e) {
//          continue;
//        }
//        PageData finalPageData = pageData;
//        while (true) {
//          try {
//            executorService.submit(
//                () -> {
//                  addPageDataToHBase(finalPageData.getUrl(), bytes, table);
//                  isUrlExist(finalPageData.getUrl(), table);
//                  addPageRankToHBase(finalPageData, table);
//                  counter++;
//                  total++;
//                });
//            break;
//          } catch (RejectedExecutionException e) {
//            Thread.sleep(40);
//          }
//        }
//      }
//      long finishTime = System.currentTimeMillis();
//      logger.info("wait for kafka in millisecond = " + (finishTimeKafka - startTimeKafka));
//      logger.info("add to HBase = " + counter);
//      logger.info("add to HBase per Second. = " + counter / ((finishTime - startTime) / 1000.));
//      logger.info(
//          "overall time for adding to HBase per Second = "
//              + total / ((finishTime - firstStartTime) / 1000.));
//      counter = 0;
//      try {
//        TimeUnit.MILLISECONDS.sleep(5);
//      } catch (InterruptedException ignored) {
//      }
//    }
//  }
//
//  @Deprecated
//  private void addPageDataToBatch(String link, byte[] pageData, Table table) {
//    try {
//      Get get =
//          new Get(toBytes(DigestUtils.md5Hex(link)))
//              .addColumn(toBytes(pageDataFamily), toBytes("pageData"));
//      if (!table.exists(get)) {
//        Put p = new Put(toBytes(DigestUtils.md5Hex(link)));
//        p.addColumn(toBytes(pageDataFamily), toBytes("pageData"), pageData);
//        batch.add(p);
//      }
//    } catch (IOException e) {
//      logger.warn("some exception happen in addPageDataToBatch method!" + e);
//    }
//  }
//
//  @Deprecated
//  private void addPageDataToHBase(String link, byte[] pageData, Table table) {
//    try {
//      Get get =
//          new Get(toBytes(DigestUtils.md5Hex(link)))
//              .addColumn(toBytes(pageDataFamily), toBytes("pageData"));
//      if (!table.exists(get)) {
//        Put p = new Put(toBytes(DigestUtils.md5Hex(link)));
//        p.addColumn(toBytes(pageDataFamily), toBytes("pageData"), pageData);
//        table.put(p);
//      }
//    } catch (IOException e) {
//      logger.warn("some exception happen in addPageDataToHBase method!" + e);
//    }
//  }
//
//  @Deprecated
//  private void addPageRankToBatch(PageData pageData, Table table) {
//    String myUrl = pageData.getUrl();
//    byte[] myLinks = LinkArraySerializer.getInstance().serialize(pageData.getLinks());
//    double myPageRank = 1.000;
//    try {
//      Get get =
//          new Get(toBytes(DigestUtils.md5Hex(myUrl)))
//              .addColumn(toBytes(pageRankFamily), toBytes("myUrl"));
//      if (!table.exists(get)) {
//        Put p = new Put(toBytes(DigestUtils.md5Hex(myUrl)));
//        p.addColumn(toBytes(pageRankFamily), toBytes("myUrl"), toBytes(myUrl));
//        p.addColumn(toBytes(pageRankFamily), toBytes("myLinks"), myLinks);
//        p.addColumn(toBytes(pageRankFamily), toBytes("myPageRank"), toBytes(myPageRank));
//        batch.add(p);
//      }
//    } catch (IOException e) {
//      logger.warn("some exception happen in addPageRankToBatch method!" + e);
//    }
//  }
//
//  @Deprecated
//  private void addPageRankToHBase(PageData pageData, Table table) {
//    String myUrl = pageData.getUrl();
//    byte[] myLinks = LinkArraySerializer.getInstance().serialize(pageData.getLinks());
//    double myPageRank = 1.000;
//    try {
//      Get get =
//          new Get(toBytes(DigestUtils.md5Hex(myUrl)))
//              .addColumn(toBytes(pageRankFamily), toBytes("myUrl"));
//      if (!table.exists(get)) {
//        Put p = new Put(toBytes(DigestUtils.md5Hex(myUrl)));
//        p.addColumn(toBytes(pageRankFamily), toBytes("myUrl"), toBytes(myUrl));
//        p.addColumn(toBytes(pageRankFamily), toBytes("myLinks"), myLinks);
//        p.addColumn(toBytes(pageRankFamily), toBytes("myPageRank"), toBytes(myPageRank));
//        table.put(p);
//      }
//    } catch (IOException e) {
//      logger.warn("some exception happen in addPageRankToHBase method!" + e);
//    }
//  }
//
//  public Configuration getConfig() {
//    return config;
//  }
//
//  @Deprecated
//  public double getPageRank(String link){
//    Get get = new Get(toBytes(DigestUtils.md5Hex(link)));
//    get.addColumn(toBytes("pageRank"), toBytes("myPageRank"));
//    try {
//      return Bytes.toDouble(defTable.get(get).getValue(toBytes("pageRank"), toBytes("myPageRank")));
//    } catch (IOException | NullPointerException e) {
//      return 0.50;
//    }
//  }
//
//  @Deprecated
//  public long getReferences(String link){
//    Get get = new Get(toBytes(DigestUtils.md5Hex(link)));
//    get.addColumn(toBytes("pageData"), toBytes("references"));
//    try {
//      return Bytes.toLong(defTable.get(get).getValue(toBytes("pageData"), toBytes("references")));
//    } catch (IOException | NullPointerException e) {
//      return 8;
//    }
//  }
//
//  public ResultScanner getResultScanner(String tableName, String familyName) throws IOException {
//    Scan scan = new Scan();
//    scan.setCaching(500);
//    scan.setCacheBlocks(false);
//    scan.addFamily(Bytes.toBytes(familyName));
//    return defConn.getTable(TableName.valueOf(tableName)).getScanner(scan);
//  }
//
//  public ResultScanner getResultScanner(String tableName, String familyName, String columnName) throws IOException {
//    Scan scan = new Scan();
//    scan.setCaching(500);
//    scan.setCacheBlocks(false);
//    scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
//    return defConn.getTable(TableName.valueOf(tableName)).getScanner(scan);
//  }
//
//  public void putToHBase(String tableName, String family, byte[] column, byte[] key, byte[] value)
//      throws IOException {
//    Put put = new Put(key).addColumn(Bytes.toBytes(family), column, value);
//    defConn.getTable(TableName.valueOf(tableName)).put(put);
//  }
//
//  public Result getFromHBase(String tableName, String family, byte[] column, byte[] key)
//      throws IOException {
//    Get get = new Get(key).addColumn(Bytes.toBytes(family), column);
//    return defConn.getTable(TableName.valueOf(tableName)).get(get);
//  }
//}
//
