package ir.sahab.nimroo.serialization;

import ir.sahab.nimroo.model.Link;
import ir.sahab.nimroo.model.PageData;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class LinkArraySerializerTest {

  @Test @Ignore("@armin TODO") // TODO @armin
  public void serializeAndDeserializeTeste() {
    PageData before = new PageData();
    byte[] bytes = LinkArraySerializer.getInstance().serialize(before.getLinks());
    ArrayList<Link> links=null;
    try {
      links = LinkArraySerializer.getInstance().deserialize(bytes);
    } catch (com.github.os72.protobuf351.InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    Assert.assertNotNull(links);
    Assert.assertEquals(links.get(0).getLink(), "http://www.test1.org/domains/example");
    Assert.assertEquals(links.get(0).getAnchor(), "More information...0");
    Assert.assertEquals(links.get(1).getLink(), "http://www.test4.org/domasns/example");
    Assert.assertEquals(links.get(1).getAnchor(), "More information...3");
    Assert.assertEquals(links.get(2).getLink(), "http://www.test3.org/domasns/example");
    Assert.assertEquals(links.get(2).getAnchor(), "More information...2");
    Assert.assertEquals(links.get(3).getLink(), "http://www.test2.org/domasns/example");
    Assert.assertEquals(links.get(3).getAnchor(), "More information...1");
  }
}