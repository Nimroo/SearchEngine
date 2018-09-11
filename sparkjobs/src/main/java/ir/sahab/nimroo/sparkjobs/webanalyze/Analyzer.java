package ir.sahab.nimroo.sparkjobs.webanalyze;

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

/*
		KeywordExtractor keywordExtractor = new KeywordExtractor("urlKeyword",
				"keywords", "finalDomKeyword", "keywords");
		keywordExtractor.extractKeywords();
*/

/*
		KeywordRelationFinder keywordRelationFinder = new KeywordRelationFinder(
				"domain", "domainGraph", "finalDomKeyword",
				"keywords", "keywordRelation", "relations");
		keywordRelationFinder.findKeywordRelation();
*/

		DomainReverser domainReverser = new DomainReverser("domain", "domainGraph",
				"reverseDomain", "domainGraph");
		domainReverser.reverseDomains();

	}
}
