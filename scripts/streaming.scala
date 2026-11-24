import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

val stream = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "accounting,research,sales,operations")
  .option("startingOffsets", "earliest")
  .load()

val parsed = stream.selectExpr("CAST(value AS STRING) AS value")
  .select(split(col("value"), ",").as("f"))
  .select(
    col("f").getItem(0).cast("int").as("user_id"),
    col("f").getItem(1).cast("timestamp").as("date_time"),
    col("f").getItem(2).as("description"),
    col("f").getItem(3).as("type"),
    col("f").getItem(4).cast("int").as("count"),
    col("f").getItem(5).cast("decimal(10,2)").as("payment")
  )

val query = parsed.writeStream
  .trigger(Trigger.ProcessingTime("1 second"))
  .foreachBatch { (batch: DataFrame, id: Long) =>
    batch.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "project3", "table" -> "expenses"))
      .mode("append").save()
  }
  .option("checkpointLocation", "/tmp/p3-checkpoint")
  .start()