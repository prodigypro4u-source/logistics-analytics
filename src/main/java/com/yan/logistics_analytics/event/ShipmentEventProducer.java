package com.yan.logistics_analytics.event;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;

@Service
public class ShipmentEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;

    public ShipmentEventProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            JsonMapper jsonMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
    }

    public CompletableFuture<SendResult<String, String>> publish(
            ShipmentEvent event) {

        String json = jsonMapper.writeValueAsString(event);

        return kafkaTemplate.send(
                "shipment-events",
                event.shipmentId(),
                json
        );
    }
}