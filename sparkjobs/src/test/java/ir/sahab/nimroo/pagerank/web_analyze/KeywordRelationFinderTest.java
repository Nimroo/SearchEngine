package ir.sahab.nimroo.pagerank.web_analyze;

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

import static org.junit.Assert.*;

public class KeywordRelationFinderTest {
	private static JavaSparkContext javaSparkContext;

	@BeforeClass
	public static void sparkConfiguration() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Keyword Relation Finder Test");
		sparkConf.setMaster("local[*]");

		javaSparkContext = new JavaSparkContext(sparkConf);
	}

	@Test
	public void extractKeywordPointsTest() {
		KeywordRelationFinder keywordRelationFinder = new KeywordRelationFinder();

		String domainSinkDomainPath = KeywordRelationFinder.class.getClassLoader().getResource("domainSinkDomainTest.txt").getPath();
		String domainKeywordPath = KeywordRelationFinder.class.getClassLoader().getResource("domainKeywordTest.txt").getPath();

		JavaPairRDD<String, List<String>> domainSinkDomain = makeJavaRDD(domainSinkDomainPath);
		JavaPairRDD<String, List<String>> domainKeyword = makeJavaRDD(domainKeywordPath);

		JavaPairRDD<Tuple2<String, String>, Integer> ans = keywordRelationFinder.extractKeywordPoints(domainSinkDomain, domainKeyword);

		ans.collect().forEach(a -> {
			System.out.println(a._1._1 + " " + a._1._2 + " " + a._2);
		});
	}


	private JavaPairRDD<String, List<String>> makeJavaRDD(String path) {
		JavaRDD<String> fileRDD = javaSparkContext.textFile(path);

		return fileRDD.mapToPair(line -> {
			String[] strings = line.split(" ");

			String first = strings[0];
			List<String> others = new ArrayList<>(Arrays.asList(strings).subList(1, strings.length));

			return new Tuple2<>(first, others);
		});
	}
}