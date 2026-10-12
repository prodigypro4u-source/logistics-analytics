package com.yan.logistics_analytics.event;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/analytics")
public class EventAnalyticsController {

    private final ShipmentEventRepository repository;

    public EventAnalyticsController(
            ShipmentEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/events")
    public Map<String, Long> getEventCounts() {
        return Map.of(
                "totalEvents", repository.count(),
                "deliveredEvents", repository.countByStatus("DELIVERED"),
                "inTransitEvents", repository.countByStatus("IN_TRANSIT")
        );
    }
}