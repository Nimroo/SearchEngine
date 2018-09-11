package ir.sahab.nimroo.sparkjobs.web_analyze;

public class Analyzer {

	public static void main(String[] args) {
		/*
		DomainExtractor domainExtractor = new DomainExtractor
				("crawler", "outLink", "domain", "domainGraph");
		domainExtractor.extractDomain();
		*/

		/*
		DomainPageRankLauncher domainPageRankLauncher = new DomainPageRankLauncher(
				"domain", "domainGraph", "pageRankDomain", "pageRank");
		domainPageRankLauncher.launchPageRank(40);
		*/


		KeywordExtractor keywordExtractor = new KeywordExtractor("urlKeyword",
				"keywords", "finalDomKeyword", "keywords");
		keywordExtractor.extractKeywords();


		/*
		KeywordRelationFinder keywordRelationFinder = new KeywordRelationFinder(
				"domain", "domainGraph", "domKeyword",
				"keywords", "keywordRelation", "relations");
		keywordRelationFinder.findKeywordRelation();
		*/
	}
}
