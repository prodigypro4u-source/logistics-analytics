package com.yan.logistics_analytics.event;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

public interface ShipmentEventRepository
        extends JpaRepository<ShipmentEventEntity, String> {

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO shipment_events
                (event_id, shipment_id, status, location, event_time)
            VALUES
                (:eventId, :shipmentId, :status, :location, :eventTime)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(
            @Param("eventId") String eventId,
            @Param("shipmentId") String shipmentId,
            @Param("status") String status,
            @Param("location") String location,
            @Param("eventTime") Instant eventTime
    );
    long countByStatus(String status);
}