package ir.sahab.nimroo.pagerank.pagerank;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankTest {
	private static JavaSparkContext javaSparkContext;
	private static PageRank pageRank;

	@BeforeClass
	public static void sparkConfiguration() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("test");
		sparkConf.setMaster("local[*]");

		javaSparkContext = new JavaSparkContext(sparkConf);
		pageRank = new PageRank();
	}

	@Test
	public void calcPageRankTest() {
		String p = PageRankTest.class.getClassLoader().getResource("pageRankTest.txt").getPath(); // TODO: 8/27/18 correct path
		JavaPairRDD<String, Tuple2<Double, List<String>>> sourceRankSinks = makeJavaRDD(p);

		for (int i = 0; i < 40; i++) {
			sourceRankSinks = pageRank.calcPageRank(sourceRankSinks);
		}

//		sourceRankSinks.saveAsTextFile("tmp/myPageRankOutput");
		sourceRankSinks.collect().forEach(System.out::println);
	}

/*	@Test
	public void calcPageRankTest2() {
		JavaPairRDD<String, Tuple2<Double, List<String>>> sourceRankSinks = makeJavaRDD("pageRankTest2.txt");

		for (int i = 0; i < 1; i++) {
			sourceRankSinks = pageRank.calcPageRank(sourceRankSinks);
		}

//		sourceRankSinks.saveAsTextFile("tmp/myPageRankOutput2");
		sourceRankSinks.collect().forEach(System.out::println);
	}*/

	private JavaPairRDD<String, Tuple2<Double, List<String>>> makeJavaRDD(String path) {
		JavaRDD<String> pageRankInfo = javaSparkContext.textFile(path);

		return pageRankInfo.mapToPair(pageRankInfoLine -> {
			String[] strings = pageRankInfoLine.split(" ");

			String source = strings[0];
			double rank = Double.parseDouble(strings[1]);
			List<String> sinks = new ArrayList<>(Arrays.asList(strings).subList(2, strings.length));

			return new Tuple2<>(source, new Tuple2<>(rank, sinks));
		});
	}
}