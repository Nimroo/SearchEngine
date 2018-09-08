package ir.sahab.nimroo.sparkjobs.web_analyze;

import ir.sahab.nimroo.sparkjobs.HBaseAPI;
import ir.sahab.nimroo.util.LinkNormalizer;
import org.apache.commons.codec.digest.DigestUtils;
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

public class DomainExtractor {
	private static Logger logger = Logger.getLogger(DomainExtractor.class);
	private static String inputTable, inputFamily, outputTable, outputFamily; //static for serialization todo
	private JavaSparkContext javaSparkContext;
	private HBaseAPI hBaseAPI;

	DomainExtractor() {} //for test only

	public DomainExtractor(String inputTable, String inputFamily, String outputTable, String outputFamily) {
		PropertyConfigurator.configure(DomainExtractor.class.getClassLoader().getResource("log4j.properties"));

		DomainExtractor.inputTable = inputTable;
		DomainExtractor.inputFamily = inputFamily;
		DomainExtractor.outputTable = outputTable;
		DomainExtractor.outputFamily = outputFamily;

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Domain Extractor");
		javaSparkContext = new JavaSparkContext(sparkConf);

		hBaseAPI = new HBaseAPI();
	}

	public void extractDomain() {
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
				hBaseAPI.getRDD(javaSparkContext, inputTable, inputFamily);

		JavaPairRDD<Tuple2<String,String>, Integer> domainSinkDomain = hBaseRDD.flatMapToPair(pairRow -> {
			Result result = pairRow._2;
			List<Cell> cells = result.listCells();

			String domain = null;
			List<String> sinkDomains = new ArrayList<>();
			List<Tuple2<Tuple2<String,String>, Integer>> domainDomain1 = new ArrayList<>();

			for (Cell cell:cells) {
				byte[] qualifier = CellUtil.cloneQualifier(cell);
				if (Bytes.toString(qualifier).equals("url")) {
					String url = Bytes.toString(CellUtil.cloneValue(cell));
					domain = LinkNormalizer.getDomain(url);
				}
				else {
					String sinkUrl = Bytes.toString(CellUtil.cloneQualifier(cell));
					String sinkDomain = LinkNormalizer.getDomain(sinkUrl);
					sinkDomains.add(sinkDomain);
				}
			}
			for (String sinkDomain:sinkDomains) {
				domainDomain1.add(new Tuple2<>(new Tuple2<>(domain, sinkDomain), 1));
			}

			return domainDomain1.iterator();
		});

		domainSinkDomain = domainSinkDomain.reduceByKey((a, b) -> a + b);

		// can write a mapreduce to complete rows
		JavaPairRDD<ImmutableBytesWritable, Put> hBasePuts = domainSinkDomain.mapToPair(domainSinkDomainNum -> {
			String domain = domainSinkDomainNum._1._1;
			String sinkDomain = domainSinkDomainNum._1._2;
			int edgeNumber = domainSinkDomainNum._2;

			Put put = new Put(Bytes.toBytes(DigestUtils.md5Hex(domain)));
			put.addColumn(Bytes.toBytes(outputFamily), Bytes.toBytes("domain"), Bytes.toBytes(domain));     //todo
			put.addColumn(Bytes.toBytes(outputFamily), Bytes.toBytes(sinkDomain), Bytes.toBytes(edgeNumber));  //todo

			return new Tuple2<>(new ImmutableBytesWritable(), put);
		});

		Job job = hBaseAPI.getJob(outputTable);
		hBasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}
}
