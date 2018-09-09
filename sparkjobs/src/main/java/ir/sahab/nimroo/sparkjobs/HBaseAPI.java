package ir.sahab.nimroo.sparkjobs;

import ir.sahab.nimroo.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class HBaseAPI {

	public HBaseAPI() {
		Config.load();
	}

	public JavaPairRDD<ImmutableBytesWritable, Result> getRDD(
			JavaSparkContext javaSparkContext, String inputTable, String inputFamily) {

		Configuration hBaseConfiguration = HBaseConfiguration.create();
		hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, inputTable);
		hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, inputFamily);
		hBaseConfiguration.addResource(Config.hBaseSite);
		hBaseConfiguration.addResource(Config.hadoopCoreSite);

		return javaSparkContext.newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
	}

	public JavaPairRDD<ImmutableBytesWritable, Result> getRDD(JavaSparkContext javaSparkContext,
			String inputTable, String inputFamily, String scanStartRow, String scanStopRow) {
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setStartRow(Bytes.toBytes(scanStartRow));
		scan.setStopRow(Bytes.toBytes(scanStopRow));
		scan.setCacheBlocks(false);
		scan.addFamily(Bytes.toBytes(inputFamily));

		Configuration hBaseConfiguration = HBaseConfiguration.create();
		hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, inputTable);
		hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, inputFamily);
		try {
			hBaseConfiguration.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
		} catch (IOException e) {
			e.printStackTrace();
		}
		hBaseConfiguration.addResource(Config.hBaseSite);
		hBaseConfiguration.addResource(Config.hadoopCoreSite);

		return javaSparkContext.newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
	}

	public Job getJob(String outputTable) {
		Configuration hBaseConfiguration = HBaseConfiguration.create(); // todo consider
		hBaseConfiguration.addResource(Config.hadoopCoreSite);
		hBaseConfiguration.addResource(Config.hBaseSite);

		Job job = null;
		try {
			job = Job.getInstance(hBaseConfiguration);
			job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);
			job.setOutputFormatClass(TableOutputFormat.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return job;
	}
}
