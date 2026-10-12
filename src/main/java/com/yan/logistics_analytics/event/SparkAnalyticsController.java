package com.yan.logistics_analytics.event;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/analytics/spark")
public class SparkAnalyticsController {

    private static final String JOB_ID = "shipment-status-v1";

    private final JdbcTemplate jdbcTemplate;

    public SparkAnalyticsController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/events")
    public SparkAnalyticsResponse getEventCounts() {

        String sql = """
                SELECT status, SUM(event_count) AS event_count
                FROM spark_batch_counts
                WHERE job_id = ?
                GROUP BY status
                ORDER BY status
                """;

        List<StatusCount> counts = jdbcTemplate.query(
                sql,
                (row, rowNumber) -> new StatusCount(
                        row.getString("status"),
                        row.getLong("event_count")
                ),
                JOB_ID
        );

        return new SparkAnalyticsResponse(JOB_ID, counts);
    }

    public record StatusCount(
            String status,
            long eventCount
    ) {
    }

    public record SparkAnalyticsResponse(
            String jobId,
            List<StatusCount> counts
    ) {
    }
}