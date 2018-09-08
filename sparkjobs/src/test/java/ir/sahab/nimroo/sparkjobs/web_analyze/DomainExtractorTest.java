package ir.sahab.nimroo.sparkjobs.web_analyze;

import ir.sahab.nimroo.util.LinkNormalizer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DomainExtractorTest {
	private static JavaSparkContext javaSparkContext;

	@BeforeClass
	public static void sparkConfiguration() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Domain Extractor Test");
		sparkConf.setMaster("local[*]");

		javaSparkContext = new JavaSparkContext(sparkConf);
	}

	@Test
	public void testExtractDomainFromTextFile() {
		DomainExtractor domainExtractor = new DomainExtractor();
		String urlSinkUrlTest = DomainExtractor.class.getClassLoader().getResource("urlSinkUrlTest.txt").getPath();
		JavaRDD<String> fileRDD = javaSparkContext.textFile(urlSinkUrlTest);

		JavaPairRDD<Tuple2<String, String>, Integer> domainSinkDomain = fileRDD.flatMapToPair(line -> {
			String[] strings = line.split(" ");
			String domain = LinkNormalizer.getDomain(strings[0]);
			List<Tuple2<Tuple2<String,String>, Integer>> domainDomain1 = new ArrayList<>();

			for (int i = 1; i < strings.length; i++) {
				domainDomain1.add(new Tuple2<>(new Tuple2<>(domain, LinkNormalizer.getDomain(strings[i])), 1));
			}

			return domainDomain1.iterator();
		});

		domainSinkDomain = domainSinkDomain.reduceByKey((a, b) -> a + b);

		domainSinkDomain.collect().forEach(result -> {
			System.out.println(result._1._1 + " " + result._1._2 + " " + result._2);
		});
	}
}