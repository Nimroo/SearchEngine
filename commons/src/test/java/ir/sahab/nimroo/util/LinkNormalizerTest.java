package ir.sahab.nimroo.util;

import org.junit.Test;

import static org.testng.Assert.*;

public class LinkNormalizerTest {

	@Test
	public void getNormalizedUrlTest() {
		assertEquals(LinkNormalizer.getNormalizedUrl("http://www.google.com"), "http://www.google.com");
		assertEquals(LinkNormalizer.getNormalizedUrl("http://www.google.com/"), "http://www.google.com");
		assertEquals(LinkNormalizer.getNormalizedUrl("HTTP://WWW.GOOGLE.COM"), "http://www.google.com");
		assertEquals(LinkNormalizer.getNormalizedUrl("Http://www.Google.com/QWERTY/aBc.html//"),
				"http://www.google.com/qwerty/abc.html");
	}

	@Test
	public void getSimpleUrlTest() {
		assertEquals(LinkNormalizer.getSimpleUrl("http://www.google.com"), "google.com");
		assertEquals(LinkNormalizer.getSimpleUrl("http://www.google.com/"), "google.com");
		assertEquals(LinkNormalizer.getSimpleUrl("HTTP://WWW.GOOGLE.COM"), "google.com");
		assertEquals(LinkNormalizer.getSimpleUrl("Http://www.Google.com/QWERTY/aBc.html//"),
				"google.com/qwerty/abc.html");
	}

	@Test
	public void getDomainTest() {
		assertEquals(LinkNormalizer.getDomain("http://google.com/abcd"), "http://google.com");
		assertEquals(LinkNormalizer.getDomain("https://google.com/abcd"), "https://google.com");
		assertEquals(LinkNormalizer.getDomain("http://google.com/abcd/adsf/sdrh/../wqer.html"), "http://google.com");
		assertEquals(LinkNormalizer.getDomain("https://login.yahoo.com/abcd"), "https://login.yahoo.com");
	}
}