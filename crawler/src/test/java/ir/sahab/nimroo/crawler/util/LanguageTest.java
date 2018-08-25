package ir.sahab.nimroo.crawler.util;

import ir.sahab.nimroo.model.PageData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class LanguageTest {

  @Test
  public void detectorWithValidInput0() {
    try {
      Language.getInstance().init();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Assert.assertTrue(Language.getInstance().detector("hello world !"));
  }

  @Test
  public void detectorWithValidInput1() {
    try {
      Language.getInstance().init();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Assert.assertFalse(Language.getInstance().detector("Deviner une énigme"));
  }

  @Test @Ignore("todo @armin") // TODO @armin
  public void detectorWithValidInput2() {

    PageData pageData = new PageData();
    try {
      Language.getInstance().init();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Assert.assertTrue(Language.getInstance().detector(pageData.getText().substring(0,java.lang.Math.min(pageData.getText().length(), 1000))));
  }
}
