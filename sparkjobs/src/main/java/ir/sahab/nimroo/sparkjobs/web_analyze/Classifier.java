package ir.sahab.nimroo.sparkjobs.web_analyze;

import ir.sahab.nimroo.sparkjobs.HBaseAPI;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Classifier {
	private static Logger logger = Logger.getLogger(Classifier.class);
	private String inputTableDomain, inputFamilyDomain, inputTableWordAndClass,
			inputFamilyWordAndClass, outputTable, outputFamily;
	private HBaseAPI hBaseAPI;
	private JavaSparkContext javaSparkContext;

	public Classifier(String inputTableDomain, String inputFamilyDomain, String inputTableWordAndClass,
	                  String inputFamilyWordAndClass, String outputTable, String outputFamily) {
		this.inputTableDomain = inputTableDomain;
		this.inputFamilyDomain = inputFamilyDomain;
		this.inputTableWordAndClass = inputTableWordAndClass;
		this.inputFamilyWordAndClass = inputFamilyWordAndClass;
		this.outputTable = outputTable;
		this.outputFamily = outputFamily;

		hBaseAPI = new HBaseAPI();

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Classifier");
		javaSparkContext = new JavaSparkContext(sparkConf);
	}

	public void classify() {
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
				hBaseAPI.getRDD(javaSparkContext, inputTableDomain, inputFamilyDomain);

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD2 =
				hBaseAPI.getRDD(javaSparkContext, inputTableWordAndClass, inputFamilyWordAndClass);

		JavaPairRDD<String, List<String>> domainKeywordsRDD = hBaseRDD.mapToPair(pairRow -> {
			Result result = pairRow._2;
			List<Cell> cells = result.listCells();
			String domain = null;
			List<String> keywords = new ArrayList<>();

			for (Cell cell: cells) {
				if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("domain")) {
					domain = Bytes.toString(CellUtil.cloneValue(cell));
				}
				else {
					String keyword = Bytes.toString(CellUtil.cloneQualifier(cell));
					keywords.add(keyword);
				}
			}

			return new Tuple2<>(domain, keywords);
		});

		JavaPairRDD<String, List<String>> keywordDomainsRDD = getKeywordDomainsRDD(domainKeywordsRDD);

		//todo
	}



	private JavaPairRDD<String, List<String>> getKeywordDomainsRDD(JavaPairRDD<String, List<String>> domainKeywordsRDD) {
		JavaPairRDD<String, List<String>> keywordDomainsRDD = domainKeywordsRDD.flatMapToPair(domainKeywords -> {
			String domain = domainKeywords._1;
			List<String> keywords = domainKeywords._2;
			List<Tuple2<String, List<String>>> ans = new ArrayList<>();
			List<String> domainList = new ArrayList<>();
			domainList.add(domain);

			for (String keyword: keywords) {
				ans.add(new Tuple2<>(keyword, domainList));
			}

			return ans.iterator();
		});

		return keywordDomainsRDD.reduceByKey((list1, list2) -> {
			if (list1.size() > list2.size()) {
				list1.addAll(list2);
				return list1;
			}
			list2.addAll(list1);
			return list2;
		});
	}
}
