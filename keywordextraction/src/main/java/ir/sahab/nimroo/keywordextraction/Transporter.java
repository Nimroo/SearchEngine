package ir.sahab.nimroo.keywordextraction;

public class Transporter {
	public static void main(String[] args) {
		ElasticToHBaseKeywordTransfer elasticToHBaseKeywordTransfer =
				new ElasticToHBaseKeywordTransfer("crawler", "outLink",
						"webpage", "urlKeyword", "keywords");
		elasticToHBaseKeywordTransfer.transferKeywords();
	}
}
