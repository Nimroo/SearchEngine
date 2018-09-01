package ir.sahab.nimroo.rss;

import ir.sahab.nimroo.Config;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.xml.sax.SAXException;

public class Controller {

  private static Logger logger;
  private static Controller ourInstance = new Controller();

  public static Controller getInstance() {
    return ourInstance;
  }

  private Controller() {
    PropertyConfigurator.configure(
        Controller.class.getClassLoader().getResource("log4j.properties"));
    logger = Logger.getLogger(Controller.class);
  }

  public static void main(String[] args){
        Config.load();
        RssConfig.load();
        RSSService rssService = new RSSService();
        rssService.runNewsUpdater();
  }
}
