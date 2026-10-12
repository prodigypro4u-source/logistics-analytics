package com.yan.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.to_timestamp;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

public class ShipmentStreamingJob {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("Shipment Streaming Analytics")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            StructType schema = new StructType()
                    .add("eventId", DataTypes.StringType)
                    .add("shipmentId", DataTypes.StringType)
                    .add("status", DataTypes.StringType)
                    .add("location", DataTypes.StringType)
                    .add("eventTime", DataTypes.StringType);

            Dataset<Row> messages = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "shipment-events")
                    .option("startingOffsets", "latest")
                    .load();

            Dataset<Row> events = messages
        .select(
                from_json(
                        col("value").cast("string"),
                        schema
                ).alias("event")
        )
        .select("event.*")
        .withColumn("eventTime", to_timestamp(col("eventTime")))
        .filter(
                col("eventId").isNotNull()
                        .and(col("status").isNotNull())
                        .and(col("eventTime").isNotNull())
        );

Dataset<Row> uniqueEvents = events
        .withWatermark("eventTime", "10 minutes")
        .dropDuplicatesWithinWatermark("eventId");

StreamingQuery query = uniqueEvents.writeStream()
        .outputMode("append")
        .foreachBatch((Dataset<Row> batch, Long batchId) -> {
    saveBatchCounts(batch, batchId);
})
        .option(
                "checkpointLocation",
                "/checkpoints/shipment-status-v1"
        )
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start();

query.awaitTermination();
        } finally {
            spark.stop();
        }
    }

    private static void saveBatchCounts(
        Dataset<Row> batch,
        long batchId) throws Exception {

    List<Row> counts = batch.groupBy("status")
            .count()
            .collectAsList();

    if (counts.isEmpty()) {
        System.out.println("No unique events in batch " + batchId);
        return;
    }

    String sql = """
            INSERT INTO spark_batch_counts
                (job_id, batch_id, status, event_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (job_id, batch_id, status)
            DO UPDATE SET
                event_count = EXCLUDED.event_count,
                updated_at = NOW()
            """;

    try (Connection connection = DriverManager.getConnection(
            "jdbc:postgresql://host.docker.internal:5434/logistics_analytics",
            "analytics",
            System.getenv("ANALYTICS_DB_PASSWORD"))) {

        connection.setAutoCommit(false);

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            for (Row row : counts) {
                statement.setString(1, "shipment-status-v1");
                statement.setLong(2, batchId);
                statement.setString(3, row.getString(0));
                statement.setLong(4, row.getLong(1));
                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();

        } catch (Exception error) {
            connection.rollback();
            throw error;
        }
    }

    System.out.println("Saved counts for batch " + batchId);
}
}