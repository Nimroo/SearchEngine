package ir.sahab.nimroo.pagerank.web_analyze;

import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.util.LinkNormalizer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class KeywordExtractor {
	private static Logger logger = Logger.getLogger(KeywordExtractor.class);
	private static Configuration hBaseConfiguration = null;

	public void extractKeywords(String inputTable, String inputFamily, String outputTable, String outputFamily) {
		Config.load();
		PropertyConfigurator.configure(KeywordExtractor.class.getClassLoader().getResource("log4j.properties"));

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Keyword Extractor");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		hBaseConfiguration = HBaseConfiguration.create();
		hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, inputTable);
		hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, inputFamily);
		hBaseConfiguration.addResource(Config.hBaseSite);
		hBaseConfiguration.addResource(Config.hadoopCoreSite);

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
				javaSparkContext.newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class,
						ImmutableBytesWritable.class, Result.class);

		JavaPairRDD<Tuple2<String,String>, Double> domainWordScore = hBaseRDD.flatMapToPair(pairRow -> {
			Result result = pairRow._2;
			List<Cell> cells = result.listCells();

			String domain = null;
			List<Tuple2<Tuple2<String,String>, Double>> domainWordFirstScore = new ArrayList<>();

			for (Cell cell:cells) {
				byte[] qualifier = CellUtil.cloneQualifier(cell);
				if (Bytes.toString(qualifier).equals("url")) {
					String url = Bytes.toString(CellUtil.cloneValue(cell));
					domain = LinkNormalizer.getDomain(url);
				}
				else {
					String word = Bytes.toString(CellUtil.cloneQualifier(cell));
					double score = Bytes.toDouble(CellUtil.cloneValue(cell));
					domainWordFirstScore.add(new Tuple2<>(new Tuple2<>(domain, word), score));
				}
			}

			return domainWordFirstScore.iterator();
		});

		domainWordScore = domainWordScore.reduceByKey((a, b) -> a + b);

		//-----------------------------------reducing number of keywords to 5----------------------------
		JavaPairRDD<String, List<Tuple2<String, Double>>> a = domainWordScore.mapToPair(pair -> {
			List<Tuple2<String, Double>> list = new ArrayList<>();
			list.add(new Tuple2<>(pair._1._2, pair._2));
			return new Tuple2<>(pair._1._1, list);
		});
		a = a.reduceByKey((list1, list2) -> {
			if (list1.size() > list2.size()) {
				list1.addAll(list2);
				return list1;
			}
			list2.addAll(list1);
			return list2;
		});
		a = a.mapToPair(pair -> {
			List<Tuple2<String, Double>> tuple2List = pair._2;

			tuple2List.sort(Comparator.comparing(o -> o._2));

			tuple2List = tuple2List.subList(Math.max(0, tuple2List.size() - 5), tuple2List.size());

			return new Tuple2<>(pair._1, tuple2List);
		});

		JavaPairRDD<ImmutableBytesWritable, Put> hBasePuts = a.mapToPair(domainSinkDomainScores -> {
			String domain = domainSinkDomainScores._1;
			List<Tuple2<String, Double>> keywords = domainSinkDomainScores._2;

			Put put = new Put(DigestUtils.md5Hex(domain).getBytes());
			put.addColumn(Bytes.toBytes(outputFamily), Bytes.toBytes("domain"), Bytes.toBytes(domain));
			for (Tuple2<String, Double> keyword:keywords) {
				put.addColumn(Bytes.toBytes(outputFamily), Bytes.toBytes(keyword._1), Bytes.toBytes(keyword._2));
			}

			return new Tuple2<>(new ImmutableBytesWritable(), put);
		});


/*		JavaPairRDD<ImmutableBytesWritable, Put> hBasePuts = domainWordScore.mapToPair(domainSinkDomainNum -> {
			String domain = domainSinkDomainNum._1._1;
			String word = domainSinkDomainNum._1._2;
			double score = domainSinkDomainNum._2;

			Put put = new Put(DigestUtils.md5Hex(domain).getBytes());
			put.addColumn(Bytes.toBytes(outputFamily), Bytes.toBytes("domain"), Bytes.toBytes(domain));
			put.addColumn(Bytes.toBytes(outputFamily), Bytes.toBytes(word), Bytes.toBytes(score));

			return new Tuple2<>(new ImmutableBytesWritable(), put);
		});
*/

		Job job = null;
		try {
			job = Job.getInstance(hBaseConfiguration);
			job.setOutputFormatClass(TableOutputFormat.class);
			job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);
		} catch (IOException e) {
			//e.printStackTrace();
		}

		hBasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}
}
