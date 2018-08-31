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
  private String tempLinkArray[] = new String[Config.shuffleSize];
  private BlockingQueue<String> linkQueueForShuffle = new ArrayBlockingQueue<>(Config.shufflerQueueSize);
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
    if(linkQueueForShuffle.size() > Config.shuffleSize) {
        synchronized (LOCK_FOR_WAIT_AND_NOTIFY_PRODUCING) {
            LOCK_FOR_WAIT_AND_NOTIFY_PRODUCING.notify();
        }
        logger.info("Notifying !!!");
    }
  }

  private void produceAll() throws InterruptedException {
    synchronized (lock) {
      if (linkQueueForShuffle.size() < Config.shuffleSize) {
        return;
      }

      for (int i = 0; i < Config.shuffleSize; i++) {
        tempLinkArray[i] = linkQueueForShuffle.take();
      }

      for (int i = 0; i < 1000; i++) {
        logger.info("number of produced links:" + i * 100);
        for (int j = i; j < Config.shuffleSize; j += 1000) {
          kafkaLinkProducer.send(Config.kafkaLinkTopicName, null, tempLinkArray[j]);
        }
      }
    }
  }
}
