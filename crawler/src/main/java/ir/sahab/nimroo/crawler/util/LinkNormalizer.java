package ir.sahab.nimroo.crawler.util;

public class LinkNormalizer {

	public static String normalize(String url) {
		String normalized = url.toLowerCase();
		int last = normalized.length();
		while (last > 0 && normalized.charAt(last - 1) == '/'){
			last--;
		}
		normalized = normalized.substring(0, last);
		return normalized;
	}
}
