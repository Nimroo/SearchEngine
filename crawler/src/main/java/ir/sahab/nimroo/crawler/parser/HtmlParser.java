package ir.sahab.nimroo.crawler.parser;

import ir.sahab.nimroo.util.LinkNormalizer;
import ir.sahab.nimroo.model.Meta;
import ir.sahab.nimroo.model.PageData;
import ir.sahab.nimroo.model.Link;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class HtmlParser {
  private PageData pageData = new PageData();
  private Set<Link> linkSet = new HashSet<>();
  private ArrayList<Meta> metas = new ArrayList<>();
  private String urlString;

  /** parses a html string and returns PageData */
  public PageData parse(String urlString, String htmlString) {
    pageData.setUrl(urlString);
    this.urlString = urlString;

    Document document = Jsoup.parse(htmlString);
    Element bodyElement = document.select("body").first();

    pageData.setTitle(document.title());
    if (bodyElement != null)
      pageData.setText(bodyElement.text());

    Elements aElements = document.select("a");

    for (Element aElement : aElements) {
      String href = aElement.attr("href");
      href = LinkNormalizer.getNormalizedUrl(href);

      href = getCompleteUrl2(urlString, href);
      if (!isValid(href)) {
        continue;
      }
      if (href.startsWith("http://www.")){
        href = "http://" + href.substring(11);
      }
      if (href.startsWith("https://www.")) {
        href = "https://" + href.substring(12);
      }
      String anchor = aElement.text();
      Link link = new Link();
      link.setAnchor(anchor);
      link.setLink(href);

      linkSet.add(link);
    }
    Link link = new Link();
    link.setLink(urlString);
    linkSet.remove(link);

    pageData.setLinks(new ArrayList<>(linkSet));

    Elements metaElements = document.select("meta");
    for (Element metaElement : metaElements) {
      Meta meta = new Meta();
      meta.setCharset(metaElement.attr("charset"));
      meta.setContent(metaElement.attr("content"));
      meta.setHttpEquiv(metaElement.attr("http-equiv"));
      meta.setName(metaElement.attr("name"));
      meta.setScheme(metaElement.attr("scheme"));

      metas.add(meta);
    }

    pageData.setMetas(metas);

    setH1H2(document);

    return pageData;
  }

  String getCompleteUrl2(String url, String relativeUrl) {
    if (relativeUrl.startsWith("http://") || relativeUrl.startsWith("https://")) {
      return relativeUrl;
    }

    if (relativeUrl.startsWith("javascript:")) {
      return "#";
    }

    if(relativeUrl.startsWith("..")) {
      try {
        return new URL(new URL(url), relativeUrl).toString();
      } catch (MalformedURLException e) {
        return "#";
      }
    }

    if(relativeUrl.startsWith("//")) {
      if (url.startsWith("http://"))
        return "http:" + relativeUrl;
      return "https:" + relativeUrl;
    }

    if(relativeUrl.startsWith("./")) {
      int lastIndex = url.lastIndexOf("/");
      if (lastIndex == -1 || (url.startsWith("http") && lastIndex <8))
        return "#";

      url = url.substring(0, lastIndex+1);
      return url + relativeUrl.substring(2);
    }

    if(relativeUrl.startsWith("/")) {
      if (url.startsWith("http://")) {
        int slashIndex = url.substring(7).indexOf('/');
        if (slashIndex != -1) {
          return url.substring(0, 7 + slashIndex) + relativeUrl;
        }
      }
      else if (url.startsWith("https://")) {
        int slashIndex = url.substring(8).indexOf('/');
        if (slashIndex != -1) {
          return url.substring(0, 8 + slashIndex) + relativeUrl;
        }
      }
      return url + relativeUrl;
    }

//    if (url.startsWith("http://")) {
//      if (url.substring(6).lastIndexOf('/') == -1) {
//        return url + "/" + relativeUrl;
//      }
//    }
//    else if (url.startsWith("https://")) {
//      if (url.substring(8).lastIndexOf('/') == -1) {
//        return url + "/" + relativeUrl;
//      }
//    }

    int lastIndex = url.lastIndexOf('/');
    if (lastIndex == -1 || (lastIndex <= 7 && url.startsWith("http"))) {
      if (relativeUrl.startsWith("/")) {
        return url + relativeUrl;
      }
      return url + "/" + relativeUrl;
    }
    url = url.substring(0, lastIndex);
    if (relativeUrl.startsWith("/")) {
      return url + relativeUrl;
    }
    return url + "/" + relativeUrl;

    //return getCompleteUrl(url, relativeUrl);
  }

  String getCompleteUrl(String url, String relativeUrl) {
    URL mainUrl;
    String host;
    if (relativeUrl.startsWith("http://") || relativeUrl.startsWith("https://")) {
      return relativeUrl;
    }

    if (relativeUrl.contains(".")) {
    	if ( (relativeUrl.indexOf('/') == -1 && (relativeUrl.indexOf('.') != relativeUrl.lastIndexOf('.') ||
			    (!relativeUrl.substring(relativeUrl.indexOf('.')).startsWith(".html") && !relativeUrl.substring(relativeUrl.indexOf('.')).startsWith(".php") )))
			    || (relativeUrl.lastIndexOf('/') > relativeUrl.indexOf('.')))
    		return relativeUrl;
    }

    try {
      mainUrl = new URL(url);
      host = mainUrl.getHost();
      if (relativeUrl.contains(host)) {
        return relativeUrl;
      }
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }

    int lastIndex = url.lastIndexOf('/');
    if (lastIndex == -1 || (lastIndex <= 7 && url.startsWith("http"))) {
      if (relativeUrl.startsWith("/")) {
        return url + relativeUrl;
      }
      return url + "/" + relativeUrl;
    }
    url = url.substring(0, lastIndex);
    if (relativeUrl.startsWith("/")) {
      return url + relativeUrl;
    }
    return url + "/" + relativeUrl;
  }

  boolean isValid(String url) {
    if (url.contains("#"))
      return false;
    if (url.equals(urlString))
      return false;
    if (url.contains("://") && !url.startsWith("http://") && !url.startsWith("https://"))
      return false;

    if (url.startsWith("mailto:"))
      return false;

    int lastSlash = url.lastIndexOf('/');
    int lastDot = url.lastIndexOf('.');
    if (url.startsWith("http://") || url.startsWith("https://")) {
	    if (lastSlash > 7 && lastDot > lastSlash &&
			    !(url.substring(lastDot).startsWith(".html") || url.substring(lastDot).startsWith(".php")))
		    return false;
    } else {
	    if (lastSlash != -1 && lastDot > lastSlash &&
			    !(url.substring(lastDot).startsWith(".html") || url.substring(lastDot).startsWith(".php")))
	    	return false;
    }
    return true;
  }

  private void setH1H2(Document document) {
    Element h1Element = document.select("h1").first();
    if (h1Element != null) {
      pageData.setH1(h1Element.text());
    }

    Elements h2Elements = document.select("h2");
    ArrayList<String> h2 = new ArrayList<>();
    for (Element h2Element: h2Elements) {
      h2.add(h2Element.text());
    }
    pageData.setH2(h2);
  }
}
