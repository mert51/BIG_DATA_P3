package com.sau;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class Main {
    private static final int[] USER_IDS = {
            7369, 7499, 7521, 7566, 7654, 7698, 7782,
            7788, 7839, 7844, 7876, 7900, 7902, 7934
    };
    private static final int[] DEPARTMENT_IDS = {
            20, 30, 30, 20, 30, 30, 10, 20, 10, 30, 20, 30, 20, 10
    };
    private static final List<String> GOODS = Arrays.asList(
            "Macaroni,Food,2,35.45",
            "TV,Appliances,1,1280.45",
            "Cinema,Entertainment,2,80.00",
            "Tea,Food,2,310.90",
            "Fridge,Appliances,2,4480.00",
            "Theatre,Entertainment,1,250.00",
            "Banana,Food,2,180.00",
            "Biscuit,Food,2,90.00",
            "Chocolate,Food,2,26.90",
            "Wrench,Hardware,3,345.00"
    );
    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.out.println("POS Server has started.");
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (!Thread.currentThread().isInterrupted()) {
                int userIndex = random.nextInt(USER_IDS.length);
                String record = USER_IDS[userIndex] + ","
                        + LocalDateTime.now().format(DATE_FORMAT) + ","
                        + GOODS.get(random.nextInt(GOODS.size()));
                String topic = Topic.getTopic(DEPARTMENT_IDS[userIndex]);

                producer.send(new ProducerRecord<>(
                        topic, String.valueOf(USER_IDS[userIndex]), record)).get();
                System.out.println(topic + " <- " + record);
                Thread.sleep(1000);
            }
        }
    }
}