package ir.sahab.nimroo.sparkjobs.web_analyze;

import ir.sahab.nimroo.sparkjobs.HBaseAPI;
import ir.sahab.nimroo.sparkjobs.pagerank.PageRank;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class DomainPageRankLauncher {
	private final HBaseAPI hBaseAPI;
	private final JavaSparkContext javaSparkContext;
	private final String inputTable, inputFamily, outputTable, outputFamily;
	private final PageRank pageRank;

	public DomainPageRankLauncher(String inputTable, String inputFamily, String outputTable, String outputFamily) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Domain PageRank");
		javaSparkContext = new JavaSparkContext(sparkConf);
		hBaseAPI = new HBaseAPI();
		pageRank = new PageRank();

		this.inputTable = inputTable;
		this.inputFamily = inputFamily;
		this.outputTable = outputTable;
		this.outputFamily = outputFamily;
	}

	public void launchPageRank(int n) {
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = hBaseAPI.getRDD(javaSparkContext, inputTable, inputFamily, "0", "001");

		JavaPairRDD<String, List<String>> domainSinkDomain = hBaseRDD.mapToPair(pairRow -> {
			Result result = pairRow._2;
			List<Cell> cells = result.listCells();
			String domain = null;
			List<String> sinkDomains = new LinkedList<>();

			for (Cell cell: cells) {
				if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("domain")) {
					domain = Bytes.toString(CellUtil.cloneValue(cell));
				}
				else {
					String sinkDomain = Bytes.toString(CellUtil.cloneQualifier(cell));
					sinkDomains.add(sinkDomain);
				}
			}

			if (!sinkDomains.contains(domain))
				sinkDomains.add(domain);        //todo consider being extra or using set

			return new Tuple2<>(domain, sinkDomains);
		});

		JavaRDD<String> domainsRDD = domainSinkDomain.map(pair -> pair._1);
		List<String> domains = domainsRDD.collect();
		Broadcast<Set<String>> setBroadcast = javaSparkContext.broadcast(new HashSet<>(domains));

		JavaPairRDD<String, Tuple2<Double, List<String>>> domainRankSinkDomain = domainSinkDomain.mapToPair(pair -> {
			String domain = pair._1;
			List<String> sinkDomains = pair._2;
			Set<String> domainsSet = setBroadcast.getValue();
			List<String> finalSinkDomains = new LinkedList<>();

			System.out.println(domainsSet.size());

			for (String sinkDomain:sinkDomains) {
				if (domainsSet.contains(sinkDomain)) {
					finalSinkDomains.add(sinkDomain);
				}
			}

			return new Tuple2<>(domain, new Tuple2<>(1d, finalSinkDomains));
		});

		setBroadcast.unpersist(); //is good?

		for (int i = 0; i < n; i++) {
			domainRankSinkDomain = pageRank.calcPageRank(domainRankSinkDomain);
		}

		Broadcast<String> outputFamilyBC = javaSparkContext.broadcast(outputFamily);

		JavaPairRDD<ImmutableBytesWritable, Put> hBasePuts = domainRankSinkDomain.mapToPair(pair -> {
			String domain = pair._1;
			double pageRank = pair._2._1;

			Put put = new Put(Bytes.toBytes(DigestUtils.md5Hex(domain)));
			put.addColumn(Bytes.toBytes(outputFamilyBC.getValue()), Bytes.toBytes("domain"), Bytes.toBytes(domain));
			put.addColumn(Bytes.toBytes(outputFamilyBC.getValue()), Bytes.toBytes("pageRank"), Bytes.toBytes(pageRank));

			return new Tuple2<>(new ImmutableBytesWritable(), put);
		});

		Job job = hBaseAPI.getJob(outputTable);
		hBasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}
}
