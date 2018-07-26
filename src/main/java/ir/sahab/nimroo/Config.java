package ir.sahab.nimroo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {


    private Config(){}
    public static String server1Address;
    public static String server2Address;
    public static int kafka1Port;
    public static int kafka2Port;
    public static int kafka3Port;
    public static int kafkaProducerBatchSize;
    public static int kafkaProducerLingerMS;
    public static String kafkaProducerAcks;
    public static String kafkaConsumerGroupId;
    public static String kafkaConsumerMaxPollRecords;
    public static String kafkaConsumerSessionTimeoutsMS;

    public static void load() {
        String appConfigPath = "app.properties";
        Properties properties = new Properties();

        try(FileInputStream fis = new FileInputStream(appConfigPath)) {
            properties.load(fis);
            Config.server1Address = properties.getProperty("server1.ip");
            Config.server2Address = properties.getProperty("server2.ip");
            Config.kafka1Port = Integer.parseInt(properties.getProperty("kafka1.port"));
            Config.kafka2Port = Integer.parseInt(properties.getProperty("kafka2.port"));
            Config.kafka3Port = Integer.parseInt(properties.getProperty("kafka3.port"));
            Config.kafkaProducerBatchSize = Integer.parseInt(properties.getProperty("kafka.producer.batch.size"));
            Config.kafkaProducerLingerMS = Integer.parseInt(properties.getProperty("kafka.producer.linger.ms"));
            Config.kafkaProducerAcks = properties.getProperty("kafka.producer.acks");
            Config.kafkaConsumerGroupId = properties.getProperty("kafka.consumer.groupId");
            Config.kafkaConsumerMaxPollRecords = properties.getProperty("kafka.consumer.max.poll.records");
            Config.kafkaConsumerSessionTimeoutsMS = properties.getProperty("kafka.consumer.session.timeout.ms");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
