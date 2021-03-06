package ir.sahab.nimroo.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import ir.sahab.nimroo.Config;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

public class NewsRepository {

  private static NewsRepository ourInstance = new NewsRepository();
  public static NewsRepository getInstance() {
    return ourInstance;
  }

  private Logger logger;
  private Configuration config;
  private String tableName;
  private Connection connection;
  private Table table;

  private String agencyFamily;

  private NewsRepository() {
    logger = Logger.getLogger(NewsRepository.class);
    config = HBaseConfiguration.create();
    if(Config.hBaseSite == null || Config.hadoopCoreSite == null){
      logger.error("please enter hbaseSite abs hadoopCoreSite with correct format.");
    }
    config.addResource(new Path(Config.hBaseSite));
    config.addResource(new Path(Config.hadoopCoreSite));
    tableName = "news";
    agencyFamily= "newsAgency";

    try {
      createTable();
    } catch (IOException e) {
      logger.error("possibly we can not get admin from HBase!", e);
    }

    try {
      connection = ConnectionFactory.createConnection(config);
      table = connection.getTable(TableName.valueOf(tableName));
    } catch (IOException e) {
      logger.error("can not get connection from HBase!", e);
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
    tableDescriptor.addFamily(new HColumnDescriptor(agencyFamily));

    byte[][] regions =
        new byte[][] {
            toBytes("4"),
            toBytes("8"),
            toBytes("c")
        };
    admin.createTable(tableDescriptor, regions);
  }

  public ResultScanner getResultScanner(String familyName) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addFamily(Bytes.toBytes(familyName));
    return table.getScanner(scan);
  }

  public ResultScanner getResultScannerWithTimeRange(String familyName, long startTime, long endTime) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addFamily(Bytes.toBytes(familyName));
    scan.setTimeRange(startTime, endTime);
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

  public ArrayList<String> getTop10Trends() throws IOException {
    ArrayList<String> trends = new ArrayList<>();
    Get get = new Get(Bytes.toBytes("top10")).addFamily(Bytes.toBytes("trendWords"));
    Result result = table.get(get);
    for(Cell cell : result.listCells())
      trends.add(Bytes.toString(CellUtil.cloneValue(cell)));
    return trends;
  }

  public Result getFromTable(String family, byte[] column, byte[] key)
      throws IOException {
    Get get = new Get(key).addColumn(Bytes.toBytes(family), column);
    return table.get(get);
  }

  public void putToTable(ArrayList<Put> puts) throws IOException {
    table.put(puts);
  }

}
