package ir.sahab.nimroo.keywordextraction;

import ir.sahab.nimroo.Config;

public class Transporter {
	public static void main(String[] args) {
		Config.load();

		ElasticToHBaseKeywordTransfer elasticToHBaseKeywordTransfer =
				new ElasticToHBaseKeywordTransfer("crawler", "outLink",
						"webpage", "urlKeyword", "keywords");
		elasticToHBaseKeywordTransfer.transferKeywords();
	}
}
