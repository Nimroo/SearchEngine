package ir.sahab.nimroo.pagerank.web_analyze;

public class Analyzer {

	public static void main(String[] args) {
		DomainExtractor domainExtractor = new DomainExtractor
				("crawler", "outLink", "domain", "domainGraph");
		domainExtractor.extractDomain();
	}
}
