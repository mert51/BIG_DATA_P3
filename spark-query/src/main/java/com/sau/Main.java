package com.sau;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class Main {
    public static void main(String[] args) {
        int userId = args.length > 0 ? Integer.parseInt(args[0]) : 7788;
        System.out.println("Spark test!");

        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkCassandra")
                .master("local[*]")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        Dataset<Row> expenses = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "project3")
                .option("table", "expenses")
                .load()
                .filter(col("user_id").equalTo(userId));
        expenses.orderBy("date_time").show(10, false);

        Row r = expenses.agg(sum("payment").alias("total")).first();
        System.out.println("user_id=" + userId + " total_expense=" + r.get(0));
        System.out.println("Expense count: " + expenses.count());

        sparkSession.stop();
        sparkSession.close();
    }
}