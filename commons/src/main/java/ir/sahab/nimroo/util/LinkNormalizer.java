package ir.sahab.nimroo.util;

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

	public static String getDomain(String url) { //url is lowercase always -- also without "www."
		String domain = url;
		if (url.startsWith("https://")) domain = url.substring(8);
		if (url.startsWith("http://")) domain = url.substring(7);
		int lastIndex = domain.indexOf('/');
		if (lastIndex == -1) lastIndex = domain.length();
		domain = domain.substring(0, lastIndex);
		if (url.startsWith("https://")) domain = "https://" + domain;
		if (url.startsWith("http://")) domain = "http://" + domain;
		return domain;
	}
}
