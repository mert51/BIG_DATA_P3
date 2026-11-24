package com.sau.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class Generator {

    // emp.csv: empno -> departman (topic adi)
    private static final int[] EMPNOS = {
            7369, 7499, 7521, 7566, 7654, 7698, 7782,
            7788, 7839, 7844, 7876, 7900, 7902, 7934};
    private static final String[] TOPICS = {
            "research", "sales", "sales", "research", "sales", "sales", "accounting",
            "research", "accounting", "sales", "research", "sales", "research", "accounting"};

    // {description, type, min fiyat, max fiyat}
    private static final Object[][] PRODUCTS = {
            {"macaroni", "food", 1.0, 10.0},
            {"bread", "food", 0.5, 5.0},
            {"fridge", "appliances", 300.0, 1500.0},
            {"vacuum", "appliances", 80.0, 400.0},
            {"cinema", "entertainment", 5.0, 25.0},
            {"concert", "entertainment", 30.0, 200.0},
            {"shirt", "clothing", 10.0, 80.0},
            {"fuel", "transport", 20.0, 100.0}
    };

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                int i = rnd.nextInt(EMPNOS.length);
                Object[] p = PRODUCTS[rnd.nextInt(PRODUCTS.length)];
                int count = rnd.nextInt(1, 6);
                double unit = rnd.nextDouble((double) p[2], (double) p[3]);
                String payment = String.format(Locale.US, "%.2f", unit * count);

                String line = EMPNOS[i] + "," + LocalDateTime.now().format(fmt) + ","
                        + p[0] + "," + p[1] + "," + count + "," + payment;

                producer.send(new ProducerRecord<>(TOPICS[i], String.valueOf(EMPNOS[i]), line));
                System.out.println(TOPICS[i] + " <- " + line);

                Thread.sleep(1000);
            }
        }
    }
}