package ir.sahab.nimroo.keywordextraction;

import ir.sahab.nimroo.Config;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticToHBaseKeywordTransfer {
	private Configuration hBaseConfiguration;
	private Table inputTable, outputTable;
	private String inputTableString, inputFamilyString, index, outputTableString, outputFamilyString;
	private Connection connection;
	private Logger logger;
	private ResultScanner resultScanner;
	private ElasticAnalysisClient elasticAnalysisClient;

	public ElasticToHBaseKeywordTransfer(String inputTableString, String inputFamilyString,
	                                     String index, String outputTableString, String outputFamilyString) {
		Config.load();
		logger = Logger.getLogger(ElasticToHBaseKeywordTransfer.class);
		elasticAnalysisClient = new ElasticAnalysisClient(Config.server3Address);

		this.inputTableString = inputTableString;
		this.inputFamilyString = inputFamilyString;
		this.index = index;
		this.outputTableString = outputTableString;
		this.outputFamilyString = outputFamilyString;

		hBaseConfiguration = HBaseConfiguration.create();
		hBaseConfiguration.addResource(Config.hBaseSite);
		hBaseConfiguration.addResource(Config.hadoopCoreSite);

		try {
			connection = ConnectionFactory.createConnection(hBaseConfiguration);
			inputTable = connection.getTable(TableName.valueOf(inputTableString));
			outputTable = connection.getTable(TableName.valueOf(outputTableString));
		} catch (IOException e) {
			logger.error("can not get connection from HBase!", e);
		}
	}

	public void transferKeywords() {
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		//scan.addFamily(Bytes.toBytes(inputFamilyString));
		scan.addColumn(Bytes.toBytes(inputFamilyString), Bytes.toBytes("url"));
		try {
			resultScanner = inputTable.getScanner(scan);
		} catch (IOException e) {
			logger.error("Couldn't get ResultScanner from scan", e);
		}

		int i = 0;
		List<String> rows = new ArrayList<>(10005); //consider using LinkedList
		Map<String, String> rowsAndUrls = new HashMap<>(10005);
		String rowKey = null;
		byte[] urlBytes = null;

		try {
			for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
				rowKey = Bytes.toString(result.getRow());
				rows.add(rowKey);
				urlBytes = result.getValue(Bytes.toBytes(inputFamilyString), Bytes.toBytes("url"));
				rowsAndUrls.put(rowKey, Bytes.toString(urlBytes));

				i++;
				if (i % 10000 == 0) {
					logger.info("results number reached to " + i);
					logger.info("last row key: " + rowKey);
					logger.info("last link: " + Bytes.toString(urlBytes));
					List<Pair<String, List<Pair<String, Double>>>> idKeywordScores =
							elasticAnalysisClient.getInterestingKeywordsForMultiDocuments(index, rows, 5);
					logger.info("result got from elasticSearch.");
					for (Pair<String, List<Pair<String, Double>>> idKeywordScore:idKeywordScores) {
						String rowKeyString = idKeywordScore.getKey();
						List<Pair<String, Double>> keywordsScore = idKeywordScore.getValue();
						String url = rowsAndUrls.get(rowKeyString);

						Put put = new Put(Bytes.toBytes(rowKeyString));
						put.addColumn(Bytes.toBytes(outputFamilyString), Bytes.toBytes("url"), Bytes.toBytes(url));
						for (Pair<String, Double> keywordScore:keywordsScore) {
							put.addColumn(Bytes.toBytes(outputFamilyString), Bytes.toBytes(keywordScore.getKey()),
									Bytes.toBytes(keywordScore.getValue()));
						}
						outputTable.put(put);
					}
					logger.info("puts to hBase completed.");
					rows.clear();
					rowsAndUrls.clear();
				}
			}

			logger.info("results number reached to " + i);
			logger.info("last row key: " + rowKey);
			logger.info("last link: " + Bytes.toString(urlBytes));
			List<Pair<String, List<Pair<String, Double>>>> idKeywordScores =
					elasticAnalysisClient.getInterestingKeywordsForMultiDocuments(index, rows, 5);
			logger.info("result got from elasticSearch.");
			for (Pair<String, List<Pair<String, Double>>> idKeywordScore:idKeywordScores) {
				String rowKeyString = idKeywordScore.getKey();
				List<Pair<String, Double>> keywordsScore = idKeywordScore.getValue();
				String url = rowsAndUrls.get(rowKeyString);

				Put put = new Put(Bytes.toBytes(rowKeyString));
				put.addColumn(Bytes.toBytes(outputFamilyString), Bytes.toBytes("url"), Bytes.toBytes(url));
				for (Pair<String, Double> keywordScore:keywordsScore) {
					put.addColumn(Bytes.toBytes(outputFamilyString), Bytes.toBytes(keywordScore.getKey()),
							Bytes.toBytes(keywordScore.getValue()));
				}
				outputTable.put(put);
			}
			logger.info("puts to hBase completed.");

		} catch (IOException e) {
			logger.error("Error in working with results: ", e);
		}
	}


	public void tansferKeywordsWithMultiTreading(String startRow) { //todo
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.addColumn(Bytes.toBytes(inputFamilyString), Bytes.toBytes("url"));
		try {
			resultScanner = inputTable.getScanner(scan);
		} catch (IOException e) {
			logger.error("Couldn't get ResultScanner from scan", e);
		}

		int i = 0;
		List<String> rows = new ArrayList<>(10005); //consider using LinkedList
		Map<String, String> rowsAndUrls = new HashMap<>(10005);
		String rowKey = null;
		byte[] urlBytes = null;

		Thread thread1 = new Thread(() -> {

		});
	}
}
