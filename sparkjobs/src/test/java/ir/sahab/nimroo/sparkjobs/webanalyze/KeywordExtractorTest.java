package ir.sahab.nimroo.sparkjobs.webanalyze;

import ir.sahab.nimroo.util.LinkNormalizer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

public class KeywordExtractorTest {
	private static JavaSparkContext javaSparkContext;

	@BeforeClass
	public static void sparkConfiguration() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Keyword Extractor Test");
		sparkConf.setMaster("local[*]");

		javaSparkContext = new JavaSparkContext(sparkConf);
	}

	@Test
	public void extractDomainListKeywordsRDDTest() {
		KeywordExtractor keywordExtractor = new KeywordExtractor();

		String urlKeywordTest = KeywordExtractor.class.getClassLoader().getResource("urlKeywordTest.txt").getPath();

		JavaRDD<String> fileRDD = javaSparkContext.textFile(urlKeywordTest);

		JavaPairRDD<Tuple2<String, String>, Double> theRDD = fileRDD.mapToPair(line -> {
			String[] strings = line.split(" ");
			return new Tuple2<>(new Tuple2<>(LinkNormalizer.getDomain(strings[0]), strings[1]), Double.parseDouble(strings[2]));
		});

		keywordExtractor.extractDomainListKeywordsRDD(theRDD).collect().forEach(domainList -> {
			String domain = domainList._1;
			List<Tuple2<String, Double>> kwScores = domainList._2;

			System.out.println(domain);
			for (Tuple2<String, Double> kwScore:kwScores) {
				System.out.println(kwScore._1 + " " + kwScore._2);
			}
		});
	}
}