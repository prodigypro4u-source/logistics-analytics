package com.yan.logistics_analytics.event;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
public class ShipmentEventConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(ShipmentEventConsumer.class);

    private final JsonMapper jsonMapper;
    private final ShipmentEventRepository repository;

    public ShipmentEventConsumer(
            JsonMapper jsonMapper,
            ShipmentEventRepository repository) {
        this.jsonMapper = jsonMapper;
        this.repository = repository;
    }

    @KafkaListener(topics = "shipment-events")
    public void consume(ConsumerRecord<String, String> record) {

        ShipmentEvent event = jsonMapper.readValue(
                record.value(),
                ShipmentEvent.class
        );

        int inserted = repository.insertIfAbsent(
                event.eventId(),
                event.shipmentId(),
                event.status(),
                event.location(),
                event.eventTime()
        );

        if (inserted == 1) {
            log.info("Saved eventId={} shipmentId={}",
                    event.eventId(), event.shipmentId());
        } else {
            log.info("Skipped duplicate eventId={}", event.eventId());
        }
    }
}