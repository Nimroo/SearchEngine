package ir.sahab.nimroo.analyzeapi;

import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcMethod;
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcParam;
import ir.sahab.nimroo.Config;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class JsonRpcAnalyzeService {
	private Configuration hBaseConfiguration;
	private String domainTableString, domainFamilyString, reverseDomainTableString, reversrDomainFamilyString;
	private Table domainTable, reverseDomainTable;
	private Connection connection;
	private Logger logger;

	public JsonRpcAnalyzeService() throws IOException{
		Config.load();
		logger = Logger.getLogger(JsonRpcAnalyzeService.class);

		domainTableString = "domain";
		domainFamilyString = "domainGraph";
		reverseDomainTableString = "reverseDomain";
		reversrDomainFamilyString = "domainGraph";

		hBaseConfiguration = HBaseConfiguration.create();
		hBaseConfiguration.addResource(Config.hBaseSite);
		hBaseConfiguration.addResource(Config.hadoopCoreSite);

		connection = ConnectionFactory.createConnection(hBaseConfiguration);
		domainTable = connection.getTable(TableName.valueOf(domainTableString));
		reverseDomainTable = connection.getTable(TableName.valueOf(reverseDomainTableString));
	}

	@JsonRpcMethod
	public List<Pair<String, Integer>> getSinkDomains(@JsonRpcParam("domain") String domain) {
		List<Pair<String, Integer>> ans = new LinkedList<>();

		String rowKey = DigestUtils.md5Hex(domain);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(domainFamilyString)); //is necessary?

		Result result;
		try {
			result = domainTable.get(get);
		} catch (IOException e) {
			logger.error("Couldn't get ResultScanner from get ", e);
			return ans;
		}
		List<Cell> cells = result.listCells();

		for (Cell cell: cells) {
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			if (!qualifier.equals("domain")) {
				int edgeNumbers = Bytes.toInt(CellUtil.cloneValue(cell));
				ans.add(new Pair<>(qualifier, edgeNumbers));
			}
		}

		return ans;
	}

	@JsonRpcMethod
	public List<Pair<String, Integer>> getSourceDomains(@JsonRpcParam("domain") String domain) {
		List<Pair<String, Integer>> ans = new LinkedList<>();

		String rowKey = DigestUtils.md5Hex(domain);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(reversrDomainFamilyString)); //is necessary?

		Result result;
		try {
			result = reverseDomainTable.get(get);
		} catch (IOException e) {
			logger.error("Couldn't get ResultScanner from get ", e);
			return ans;
		}
		List<Cell> cells = result.listCells();

		for (Cell cell: cells) {
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			if (!qualifier.equals("domain")) {
				int edgeNumbers = Bytes.toInt(CellUtil.cloneValue(cell));
				ans.add(new Pair<>(qualifier, edgeNumbers));
			}
		}

		return ans;
	}
}
