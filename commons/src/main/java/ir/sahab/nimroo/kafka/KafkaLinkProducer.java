package ir.sahab.nimroo.kafka;

import ir.sahab.nimroo.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaLinkProducer {

  private Producer<String, String> producer;

  public KafkaLinkProducer() {
    Properties props = new Properties();
    props.put(
            "bootstrap.servers",
            Config.server2Address
                    + ":"
                    + Config.kafka2Port
                    + ","
                    + Config.server3Address
                    + ":"
                    + Config.kafka3Port);
    props.put("acks", Config.kafkaProducerAcks);
    props.put("retries", 0);
    props.put("batch.size", Config.kafkaProducerBatchSize);
    props.put("linger.ms", Config.kafkaProducerLingerMS);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", StringSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  public void send(String topic, String key, String value) {
    System.err.println(topic + " - "+ value);
    producer.send(new ProducerRecord<>(topic, key, value));
  }
}
