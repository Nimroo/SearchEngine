package ir.sahab.nimroo.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import ir.sahab.nimroo.Config;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

public class CrawlerRepository {

  private static CrawlerRepository ourInstance = new CrawlerRepository();

  public static CrawlerRepository getInstance() {
    return ourInstance;
  }

  private Logger logger;
  private Configuration config;
  private String tableName;
  private Connection connection;
  private Table table;

  private String anchorFamily;
  private String outLinkFamily;
  private String markFamily;
  private String pageRankFamily;

  private CrawlerRepository() {
    logger = Logger.getLogger(CrawlerRepository.class);
    config = HBaseConfiguration.create();
    config.addResource(new Path(Config.hBaseSite));
    config.addResource(new Path(Config.hadoopCoreSite));
    tableName = "crawler";
    anchorFamily = "anchor";
    outLinkFamily = "outLink";
    markFamily = "mark";
    pageRankFamily = "pageRank";

    try {
      createTable();
    } catch (IOException e) {
      logger.error("possibly we can not get admin from HBase ir.sahab.nimroo.connection!", e);
    }

    try {
      connection = ConnectionFactory.createConnection(config);
      table = connection.getTable(TableName.valueOf(tableName));
    } catch (IOException e) {
      logger.error("can not get ir.sahab.nimroo.connection from HBase!", e);
      System.exit(499);
    }
  }

  private Admin getAdmin() {
    try {
      return ConnectionFactory.createConnection(config).getAdmin();
    } catch (IOException e) {
      return null;
    }
  }

  private void createTable() throws IOException {
    Admin admin = getAdmin();
    if (admin == null) return;
    if (admin.tableExists(TableName.valueOf(tableName))) {
      return;
    }
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
    tableDescriptor.addFamily(new HColumnDescriptor(markFamily));
    tableDescriptor.addFamily(new HColumnDescriptor(anchorFamily));
    tableDescriptor.addFamily(new HColumnDescriptor(outLinkFamily));
    tableDescriptor.addFamily(new HColumnDescriptor(pageRankFamily));

    byte[][] regions =
        new byte[][] {
          toBytes("0"),
          toBytes("1"),
          toBytes("2"),
          toBytes("3"),
          toBytes("4"),
          toBytes("5"),
          toBytes("6"),
          toBytes("7"),
          toBytes("8"),
          toBytes("9"),
          toBytes("a"),
          toBytes("b"),
          toBytes("c"),
          toBytes("d"),
          toBytes("e"),
          toBytes("f")
        };
    admin.createTable(tableDescriptor, regions);
  }

  public boolean isDuplicateUrl(String link) {
    logger.trace("isDuplicateUrl is called !!!");
    return isUrlExist(link, table);
  }

  private boolean isUrlExist(String link, Table table) {
    try {
      Get get =
          new Get(toBytes(DigestUtils.md5Hex(link)))
              .addColumn(toBytes(markFamily), toBytes("link"));
      if (!table.exists(get)) {
        Put p = new Put(toBytes(DigestUtils.md5Hex(link)));
        p.addColumn(toBytes(markFamily), toBytes("link"), toBytes(0));
        table.put(p);
        return false;
      }
      return true;
    } catch (IOException e) {
      logger.warn("some exception happen in isUrlExist method!" + e);
      return false;
    }
  }

  public ResultScanner getResultScanner(String familyName) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addFamily(Bytes.toBytes(familyName));
    return table.getScanner(scan);
  }

  public ResultScanner getResultScanner(String familyName, String columnName) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
    return table.getScanner(scan);
  }

  public void putToTable(String family, byte[] column, byte[] key, byte[] value)
      throws IOException {
    Put put = new Put(key).addColumn(Bytes.toBytes(family), column, value);
    table.put(put);
  }

  public Result getFromTable(String family, byte[] column, byte[] key) throws IOException {
    Get get = new Get(key).addColumn(Bytes.toBytes(family), column);
    return table.get(get);
  }

  public void putToTable(String family, byte[] key, ArrayList<Pair<byte[], byte[]>> columns)
      throws IOException {
    Put put = new Put(key);
    for (Pair pair : columns) {
      put.addColumn(Bytes.toBytes(family), (byte[]) pair.getFirst(), (byte[]) pair.getSecond());
    }
    table.put(put);
  }

  public double getPageRank(String link){
    try {
      return Bytes.toDouble(getFromTable(pageRankFamily,
          Bytes.toBytes("pageRank"),
          Bytes.toBytes(DigestUtils.md5Hex(link)))
          .getValue(toBytes("pageRank"), toBytes("pageRank")));
    } catch (IOException | NullPointerException e) {
      return 0.50;
    }
  }
}
