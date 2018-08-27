package ir.sahab.nimroo.crawler;

import ir.sahab.nimroo.Config;
import ir.sahab.nimroo.kafka.KafkaLinkProducer;
import org.apache.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class LinkShuffler implements Runnable {
  private final Object LOCK_FOR_WAIT_AND_NOTIFY_PRODUCING = new Object();
  private Logger logger = Logger.getLogger(LinkShuffler.class);
  private KafkaLinkProducer kafkaLinkProducer = new KafkaLinkProducer();
  private String tempLinkArray[] = new String[Config.shuffelSize];
  private BlockingQueue<String> linkQueueForShuffle = new ArrayBlockingQueue<>(Config.shuffelerQueueSize);
  private final Object lock = new Object();

  public LinkShuffler() {

  }

  @Override
  public void run() {
    while (true) {
      synchronized (LOCK_FOR_WAIT_AND_NOTIFY_PRODUCING) {
        try {
          LOCK_FOR_WAIT_AND_NOTIFY_PRODUCING.wait();
            logger.info("Notified !!!");
            produceAll();
        } catch (InterruptedException e) {
          logger.error("InterruptedException happen!", e);
        }
      }
    }
  }

  public void submitLink(String url) {
    linkQueueForShuffle.add(url);
    logger.info("submitted link: " + url);
    if(linkQueueForShuffle.size() > Config.shuffelSize) {
        synchronized (LOCK_FOR_WAIT_AND_NOTIFY_PRODUCING) {
            LOCK_FOR_WAIT_AND_NOTIFY_PRODUCING.notify();
        }
        logger.info("Notifying !!!");
    }
  }

  private void produceAll() throws InterruptedException {
    synchronized (lock) {
      if (linkQueueForShuffle.size() < Config.shuffelSize) {
        return;
      }

      for (int i = 0; i < Config.shuffelSize; i++) {
        tempLinkArray[i] = linkQueueForShuffle.take();
      }

      for (int i = 0; i < 1000; i++) {
        logger.info("number of produced links:" + i * 100);
        for (int j = i; j < Config.shuffelSize; j += 1000) {
          kafkaLinkProducer.send(Config.kafkaLinkTopicName, tempLinkArray[j], tempLinkArray[j]);
        }
      }
    }
  }
}
