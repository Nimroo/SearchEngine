package ir.sahab.nimroo.serialization;

import ir.sahab.nimroo.model.PageData;
import ir.sahab.nimroo.model.Link;
import ir.sahab.nimroo.model.Meta;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class PageDataSerializerTest {

  @Test @Ignore("@ARMIN TODO") // TODO @armin
  public void serializeAndDeserializeTest() {

    PageData before = new PageData();
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
    }
  }
}
