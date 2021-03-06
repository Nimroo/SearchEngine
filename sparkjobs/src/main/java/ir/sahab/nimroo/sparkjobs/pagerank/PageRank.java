package ir.sahab.nimroo.sparkjobs.pagerank;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

public class PageRank {
	private static final double d = 0.85;

	public JavaPairRDD<String, Tuple2<Double, List<String>>> calcPageRank(JavaPairRDD<String, Tuple2<Double, List<String>>> sourceRankAndLinks) {
		JavaPairRDD<String, Double> sinkRankRDD = sourceRankAndLinks.flatMapToPair(string_doubleListString -> {
			double pr = string_doubleListString._2._1;
			List<String> sinks = string_doubleListString._2._2;
			int out = sinks.size();

			List<Tuple2<String, Double>> sinkRankResults = new LinkedList<>();
			for (String sink: sinks) {
				sinkRankResults.add(new Tuple2<>(sink, pr/out));
			}

			return sinkRankResults.iterator();
		});

		JavaPairRDD<String, List<String>> sourceAndLinks = sourceRankAndLinks.mapToPair(string_doubleListString ->
				new Tuple2<>(string_doubleListString._1, string_doubleListString._2._2));

		JavaPairRDD<String, Double> reducedSinkRankRDD = sinkRankRDD.reduceByKey((a, b) -> a + b);

		JavaPairRDD<String, Double> finalSinkRank = reducedSinkRankRDD.mapToPair(sinkAndSum ->
				new Tuple2<>(sinkAndSum._1, (d * sinkAndSum._2) + (1 - d)));

		return finalSinkRank.join(sourceAndLinks);
	}
}