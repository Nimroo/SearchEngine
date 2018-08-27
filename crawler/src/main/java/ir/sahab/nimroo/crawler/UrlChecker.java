package ir.sahab.nimroo.crawler;

import ir.sahab.nimroo.crawler.cache.DummyUrlCache;
import ir.sahab.nimroo.hbase.HBase;
import org.apache.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class UrlChecker implements Runnable {

    private final DummyUrlCache dummyUrlCache;
    private final LinkShuffler linkShuffler;
    private BlockingQueue<String> linkQueue = new ArrayBlockingQueue<>(150000);
    private final Logger logger = Logger.getLogger(UrlChecker.class);

    public UrlChecker(DummyUrlCache dummyUrlCache, LinkShuffler linkShuffler) {
        this.dummyUrlCache = dummyUrlCache;
        this.linkShuffler = linkShuffler;
    }

    public void submit(String link) {
        if (dummyUrlCache.add(link)) {
            logger.info("checked with lru->  link: " + link);
            try {
                linkQueue.put(link);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        logger.info("UrlChecker Started");

        String link;
        while (true) {
            try {
                link = linkQueue.take();
                logger.info("UrlChecker takeLink: " + link);
                HBase.getInstance();
                if (!HBase.getInstance().isDuplicateUrl(link)) {
                    logger.info("checked with hBase->  link: " + link);
                    linkShuffler.submitLink(link);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}

