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
import java.util.List;

public class DomainExtractor {
	private static Logger logger = Logger.getLogger(DomainExtractor.class);
	private static Configuration hBaseConfiguration = null;

	public void extractDomain() {
		Config.load();
		PropertyConfigurator.configure(DomainExtractor.class.getClassLoader().getResource("log4j.properties"));

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Domain Extractor");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		hBaseConfiguration = HBaseConfiguration.create();
		hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, ""); //todo
		hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, ""); //todo
		hBaseConfiguration.addResource(Config.hBaseSite);
		hBaseConfiguration.addResource(Config.hadoopCoreSite);

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
				javaSparkContext.newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class,
						ImmutableBytesWritable.class, Result.class);

		JavaPairRDD<Tuple2<String,String>, Integer> domainSinkDomain = hBaseRDD.flatMapToPair(pairRow -> {
			Result result = pairRow._2;
			List<Cell> cells = result.listCells();

			String domain = null;
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
					domainDomain1.add(new Tuple2<>(new Tuple2<>(domain, sinkDomain), 1));
				}
			}

			return domainDomain1.iterator();
		});

		domainSinkDomain = domainSinkDomain.reduceByKey((a, b) -> a + b);

		JavaPairRDD<ImmutableBytesWritable, Put> hBasePuts = domainSinkDomain.mapToPair(domainSinkDomainNum -> {
			String domain = domainSinkDomainNum._1._1;
			String sinkDomain = domainSinkDomainNum._1._2;
			int edgeNumber = domainSinkDomainNum._2;

			Put put = new Put(DigestUtils.md5Hex(domain).getBytes());
			put.addColumn(Bytes.toBytes(""), Bytes.toBytes("domain"), Bytes.toBytes(domain));       //todo
			put.addColumn(Bytes.toBytes(""), Bytes.toBytes(sinkDomain), Bytes.toBytes(edgeNumber));     //todo

			return new Tuple2<>(new ImmutableBytesWritable(), put);
		});


		Job job = null;
		try {
			job = Job.getInstance(hBaseConfiguration);
			job.setOutputFormatClass(TableOutputFormat.class);
			job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, ""); //todo
		} catch (IOException e) {
			e.printStackTrace();
		}

		hBasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}
}




