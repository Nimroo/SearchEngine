package ir.sahab.nimroo.serialization;

import ir.sahab.nimroo.model.Link;
import ir.sahab.nimroo.model.Meta;
import ir.sahab.nimroo.model.PageData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;

public class PageDataSerializerTest {

  PageData before = new PageData();

  @Before
  public void setUp() throws Exception {
    before.setUrl("http://pagedataserializertestlink.com/salam");
    before.setTitle("salam");
    before.setText("salam bar to javan irani :))))");
    before.setH1("Saaaaaalaaam jaaaaavaaaan Iraaaaaniiiii");
    ArrayList<String> tmp = new ArrayList<>();
    tmp.add("h2_1");
    tmp.add("h2_2");
    tmp.add("h2_3");
    before.setH2(tmp);
    Link tmp2 = new Link();
    tmp2.setLink("link2");
    tmp2.setAnchor("link2_anchor");
    ArrayList<Link> links = new ArrayList<>();
    links.add(tmp2);
    before.setLinks(links);
    Meta tmp3 = new Meta();
    tmp3.setCharset("utf-8");
    tmp3.setHttpEquiv("Equiv");
    tmp3.setScheme("html");
    tmp3.setName("salam");
    tmp3.setContent("salam2");
    ArrayList tmp4 = new ArrayList<>();
    tmp4.add(tmp3);
    before.setMetas(tmp4);
  }

  @Test @Ignore("@ARMIN TODO") // TODO @armin
  public void serializeAndDeserializeTest() {

    byte[] byteArray = PageDataSerializer.getInstance().serialize(before);
    PageData after = null;
    try {
      after = PageDataSerializer.getInstance().deserialize(byteArray);
    } catch (com.github.os72.protobuf351.InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(before.getUrl(), after.getUrl());
    Assert.assertEquals(before.getText(), after.getText());
    Assert.assertEquals(before.getTitle(), after.getTitle());
    for (Link link : before.getLinks()) {
      boolean flag = false;
      for (Link link2 : after.getLinks()) {
        flag |=
            (link.getLink().equals(link2.getLink()) & link.getAnchor().equals(link2.getAnchor()));
      }
      Assert.assertTrue(flag);
    }

    for (Meta meta : before.getMetas()) {
      boolean flag = false;
      for (Meta meta2 : after.getMetas()) {
        flag |=
            (meta.getCharset().equals(meta2.getCharset())
                & meta.getContent().equals(meta2.getContent())
                & meta.getName().equals(meta2.getName())
                & meta.getHttpEquiv().equals(meta2.getHttpEquiv())
                & meta.getScheme().equals(meta2.getScheme()));
      }
      Assert.assertTrue(flag);

      for (int i = 0; i < before.getH2().size(); i++) {
        Assert.assertEquals(before.getH2().get(i), after.getH2().get(i));
      }

      Assert.assertEquals(before.getH1(), before.getH1());
    }
  }
}
