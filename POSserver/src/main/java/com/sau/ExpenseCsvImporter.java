package com.sau;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class ExpenseCsvImporter {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Usage: ExpenseCsvImporter <emp.csv> <dept.csv> <expense.csv>");
        }

        Map<Integer, String> topics = loadTopics(Path.of(args[0]), Path.of(args[1]));
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        int published = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
             BufferedReader reader = Files.newBufferedReader(Path.of(args[2]), StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IllegalArgumentException("Expense CSV is empty: " + args[2]);
            }
            Map<String, Integer> columns = columns(header);
            int userColumn = requiredColumn(columns, "empno");
            int dateColumn = requiredColumn(columns, "date_time");
            int descriptionColumn = requiredColumn(columns, "description");
            int typeColumn = requiredColumn(columns, "type");
            int countColumn = requiredColumn(columns, "count");
            int paymentColumn = requiredColumn(columns, "payment");

            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] fields = line.split(",", -1);
                int expectedColumns = columns.size();
                if (fields.length != expectedColumns) {
                    throw new IllegalArgumentException(
                            "Expected " + expectedColumns + " columns in " + args[2]
                                    + " at line " + lineNumber + ", got " + fields.length);
                }

                int userId = Integer.parseInt(fields[userColumn].trim());
                String topic = topics.get(userId);
                if (topic == null) {
                    throw new IllegalArgumentException(
                            "Employee " + userId + " from " + args[2]
                                    + " is missing from " + args[0]);
                }

                String record = String.join(",",
                        fields[userColumn].trim(),
                        fields[dateColumn].trim(),
                        fields[descriptionColumn].trim(),
                        fields[typeColumn].trim(),
                        fields[countColumn].trim(),
                        fields[paymentColumn].trim());
                producer.send(new ProducerRecord<>(topic, String.valueOf(userId), record)).get();
                published++;
            }
        }

        System.out.println("Published " + published + " expense CSV records to Kafka.");
    }

    private static Map<Integer, String> loadTopics(Path employeesFile, Path departmentsFile)
            throws Exception {
        Map<Integer, String> departmentNames = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(departmentsFile, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IllegalArgumentException("Department CSV is empty: " + departmentsFile);
            }
            Map<String, Integer> columns = columns(header);
            int idColumn = requiredColumn(columns, "deptno");
            int nameColumn = requiredColumn(columns, "dname");
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] fields = line.split(",", -1);
                departmentNames.put(
                        Integer.parseInt(fields[idColumn].trim()),
                        fields[nameColumn].trim().toLowerCase(Locale.ROOT));
            }
        }

        Map<Integer, String> topics = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(employeesFile, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IllegalArgumentException("Employee CSV is empty: " + employeesFile);
            }
            Map<String, Integer> columns = columns(header);
            int employeeColumn = requiredColumn(columns, "empno");
            int departmentColumn = requiredColumn(columns, "deptno");
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] fields = line.split(",", -1);
                int employeeId = Integer.parseInt(fields[employeeColumn].trim());
                int departmentId = Integer.parseInt(fields[departmentColumn].trim());
                String topic = departmentNames.get(departmentId);
                if (topic == null) {
                    throw new IllegalArgumentException(
                            "Department " + departmentId + " for employee " + employeeId
                                    + " is missing from " + departmentsFile);
                }
                topics.put(employeeId, topic);
            }
        }
        return topics;
    }

    private static Map<String, Integer> columns(String header) {
        String[] names = header.replace("\uFEFF", "").split(",", -1);
        Map<String, Integer> columns = new HashMap<>();
        for (int i = 0; i < names.length; i++) {
            columns.put(names[i].trim().toLowerCase(Locale.ROOT), i);
        }
        return columns;
    }

    private static int requiredColumn(Map<String, Integer> columns, String name) {
        Integer index = columns.get(name);
        if (index == null) {
            throw new IllegalArgumentException("CSV is missing required column: " + name);
        }
        return index;
    }
}
