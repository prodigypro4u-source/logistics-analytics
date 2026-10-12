package com.yan.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ShipmentBatchJob {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Shipment Event Analytics")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<Row> events = spark.read()
                    .format("jdbc")
                    .option(
                            "url",
                            "jdbc:postgresql://host.docker.internal:5434/logistics_analytics"
                    )
                    .option("dbtable", "shipment_events")
                    .option("user", "analytics")
                    .option(
                            "password",
                            System.getenv("ANALYTICS_DB_PASSWORD")
                    )
                    .option("driver", "org.postgresql.Driver")
                    .load();

            events.groupBy("status")
                    .count()
                    .orderBy("status")
                    .show(false);

        } finally {
            spark.stop();
        }
    }
}