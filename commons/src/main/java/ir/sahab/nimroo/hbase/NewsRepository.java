package ir.sahab.nimroo.hbase;

public class NewsRepository {

  private static NewsRepository ourInstance = new NewsRepository();

  public static NewsRepository getInstance() {
    return ourInstance;
  }

  private NewsRepository() {
  }
}
