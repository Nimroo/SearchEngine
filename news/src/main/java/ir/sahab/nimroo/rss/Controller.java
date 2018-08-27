package ir.sahab.nimroo.rss;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

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

  public static void main(String[] args) {
    RSSService rssService = new RSSService();
    rssService.runNewsUpdater();
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("d");
    stringBuilder.toString();

  }
}
