package ir.sahab.nimroo.crawler.util;

public class LinkNormalizer {

	public static String getNormalizedUrl(String url) {
		String normalized = url.toLowerCase();
		int last = normalized.length();
		while (last > 0 && normalized.charAt(last - 1) == '/'){
			last--;
		}
		normalized = normalized.substring(0, last);
		return normalized;
	}

	public static String getSimpleUrl(String url) {
		String link = LinkNormalizer.getNormalizedUrl(url);
		if (link.startsWith("https://")) link = link.substring(8);
		if (link.startsWith("http://"))  link = link.substring(7);
		if (link.startsWith("www."))     link = link.substring(4);
		return link;
	}
}
