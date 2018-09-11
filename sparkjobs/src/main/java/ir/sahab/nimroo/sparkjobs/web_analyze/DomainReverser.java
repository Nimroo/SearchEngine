package ir.sahab.nimroo.sparkjobs.web_analyze;

import ir.sahab.nimroo.sparkjobs.HBaseAPI;
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class DomainReverser {
	private String inputTable, inputFamily, outputTable, outputFamily;
	private JavaSparkContext javaSparkContext;
	private HBaseAPI hBaseAPI;

	public DomainReverser(String inputTable, String inputFamily, String outputTable, String outputFamily) {
		this.inputTable = inputTable;
		this.inputFamily = inputFamily;
		this.outputTable = outputTable;
		this.outputFamily = outputFamily;

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Domain Reverser (sinks)");
		javaSparkContext = new JavaSparkContext(sparkConf);

		hBaseAPI = new HBaseAPI();
	}

	public void reverseDomains() {
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
				hBaseAPI.getRDD(javaSparkContext, inputTable, inputFamily);

		JavaPairRDD<String, Tuple2<String, Integer>> sinkDomDom = hBaseRDD.flatMapToPair(pairRow -> {
			Result result = pairRow._2;
			List<Cell> cells = result.listCells();
			String domain = null;
			List<Tuple2<String, Integer>> sinkEdgeNumbers = new ArrayList<>();
			List<Tuple2<String, Tuple2<String, Integer>>> ans = new ArrayList<>();

			for (Cell cell: cells) {
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				if (qualifier.equals("domain")) {
					domain = Bytes.toString(CellUtil.cloneValue(cell));
				} else {
					sinkEdgeNumbers.add(new Tuple2<>(qualifier, Bytes.toInt(CellUtil.cloneValue(cell))));
				}
			}

			for (Tuple2<String, Integer> sinkEdgeNumber: sinkEdgeNumbers) {
				ans.add(new Tuple2<>(sinkEdgeNumber._1, new Tuple2<>(domain, sinkEdgeNumber._2)));
			}

			return ans.iterator();
		});

		Broadcast<String> outputFamilyBC = javaSparkContext.broadcast(outputFamily);
		JavaPairRDD<ImmutableBytesWritable, Put> hBasePuts = sinkDomDom.mapToPair(sinkDomDomNum -> {
			String sink = sinkDomDomNum._1;
			String dom = sinkDomDomNum._2._1;
			int num = sinkDomDomNum._2._2;

			Put put = new Put(Bytes.toBytes(DigestUtils.md5Hex(sink)));
			put.addColumn(Bytes.toBytes(outputFamilyBC.getValue()), Bytes.toBytes("domain"), Bytes.toBytes(sink));
			put.addColumn(Bytes.toBytes(outputFamilyBC.getValue()), Bytes.toBytes(dom), Bytes.toBytes(num));

			return new Tuple2<>(new ImmutableBytesWritable(), put);
		});

		Job job = hBaseAPI.getJob(outputTable);

		hBasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}
}
