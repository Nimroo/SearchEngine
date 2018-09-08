package ir.sahab.nimroo.sparkjobs.web_analyze;

public class Analyzer {

	public static void main(String[] args) {
		/*
		DomainExtractor domainExtractor = new DomainExtractor
				("crawler", "outLink", "domain", "domainGraph");
		domainExtractor.extractDomain();
		*/

		DomainPageRankLauncher domainPageRankLauncher = new DomainPageRankLauncher(
				"domain", "domainGraph", "domainPageRank", "pageRank");
		domainPageRankLauncher.launchPageRank(40);
	}
}
