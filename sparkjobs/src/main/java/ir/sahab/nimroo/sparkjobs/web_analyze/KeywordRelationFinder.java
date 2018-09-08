package ir.sahab.nimroo.sparkjobs.web_analyze;

import ir.sahab.nimroo.sparkjobs.HBaseAPI;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class KeywordRelationFinder {
	private static Logger logger = Logger.getLogger(KeywordExtractor.class);
	private static String inputTable, inputFamily, inputTable2, inputFamily2,
			outputTable, outputFamily; //static for serialization
	private HBaseAPI hBaseAPI;
	private JavaSparkContext javaSparkContext;

	KeywordRelationFinder() {}  //for test only

	public KeywordRelationFinder(String inputTable, String inputFamily, String inputTable2,
	                             String inputFamily2, String outputTable, String outputFamily) {
		PropertyConfigurator.configure(KeywordRelationFinder.class.getClassLoader().getResource("log4j.properties"));

		KeywordRelationFinder.inputTable = inputTable;
		KeywordRelationFinder.inputFamily = inputFamily;
		KeywordRelationFinder.inputTable2 = inputTable2;
		KeywordRelationFinder.inputFamily2 = inputFamily2;
		KeywordRelationFinder.outputTable = outputTable;
		KeywordRelationFinder.outputFamily = outputFamily;

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Keyword Extractor");
		javaSparkContext = new JavaSparkContext(sparkConf);

		hBaseAPI = new HBaseAPI();
	}

	public void findKeywordRelation() {
		JavaPairRDD<String, List<String>> domainSinkDomain = extractDomainAndList(inputTable, inputFamily);
		JavaPairRDD<String, List<String>> domainKeyword = extractDomainAndList(inputTable2, inputFamily2);

		JavaPairRDD<Tuple2<String, String>, Integer> domKwSinkKw = extractKeywordPoints(domainSinkDomain, domainKeyword);

		JavaPairRDD<ImmutableBytesWritable, Put> hBasePuts = domKwSinkKw.mapToPair(pairKeywordAndScore -> {
			String fromKw = pairKeywordAndScore._1._1;
			String toKw = pairKeywordAndScore._1._2;
			int number = pairKeywordAndScore._2;

			Put put = new Put(Bytes.toBytes(fromKw));
			put.addColumn(Bytes.toBytes(outputFamily), Bytes.toBytes(toKw), Bytes.toBytes(number));

			return new Tuple2<>(new ImmutableBytesWritable(), put);
		});

		Job job = hBaseAPI.getJob(outputTable);
		hBasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}


	JavaPairRDD<Tuple2<String, String>, Integer> extractKeywordPoints
			(JavaPairRDD<String, List<String>> domainSinkDomain, JavaPairRDD<String, List<String>> domainKeyword) {

		JavaPairRDD<String, Tuple2<List<String>, List<String>>> domainSinkDomainKeyword = domainSinkDomain.join(domainKeyword);

		JavaPairRDD<String, List<String>> sinkDomainListKeyword = domainSinkDomainKeyword.flatMapToPair(domSDomKw -> {
			List<String> sinkDomains = domSDomKw._2._1;
			List<String> keywords = domSDomKw._2._2;

			List<Tuple2<String, List<String>>> ans = new ArrayList<>();
			for (String sinkDomain:sinkDomains) {
				ans.add(new Tuple2<>(sinkDomain, keywords));
			}

			return ans.iterator();
		});

		sinkDomainListKeyword = sinkDomainListKeyword.reduceByKey((list1, list2) -> {
			if (list1.size() > list2.size()) {
				list1.addAll(list2);
				return list1;
			}
			list2.addAll(list1);
			return list2;
		});

		JavaPairRDD<String, Tuple2<List<String>, List<String>>> sinkDomKwDomKwSinkDom =
				sinkDomainListKeyword.join(domainKeyword);

		JavaPairRDD<Tuple2<String, String>, Integer> domKwSinkKw =
				sinkDomKwDomKwSinkDom.flatMapToPair(sinkListDomsKwListSDomKw -> {
					List<String> domsKws = sinkListDomsKwListSDomKw._2._1;
					List<String> sDomsKws = sinkListDomsKwListSDomKw._2._2;

					List<Tuple2<Tuple2<String, String>, Integer>> ans = new ArrayList<>();
					for (String domsKw:domsKws) {
						for (String sDomsKw:sDomsKws) {
							ans.add(new Tuple2<>(new Tuple2<>(domsKw, sDomsKw), 1));
						}
					}

					return ans.iterator();
				});

		return domKwSinkKw.reduceByKey((a, b) -> a + b);
	}

	private JavaPairRDD<String, List<String>> extractDomainAndList(String inputTable, String inputFamily) {
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
				hBaseAPI.getRDD(javaSparkContext, inputTable, inputFamily);

		return hBaseRDD.mapToPair(pairRow -> {
			Result result = pairRow._2;
			List<Cell> cells = result.listCells();
			String domain = null;
			List<String> sinkDomains = new ArrayList<>();

			for (Cell cell:cells) {
				if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("domain")) {
					domain = Bytes.toString(CellUtil.cloneValue(cell));
				}
				else {
					sinkDomains.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
				}
			}

			return new Tuple2<>(domain, sinkDomains);
		});
	}
}
