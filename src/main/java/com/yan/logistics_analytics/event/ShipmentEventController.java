package com.yan.logistics_analytics.event;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/events")
public class ShipmentEventController {

    private final ShipmentEventProducer producer;

    public ShipmentEventController(ShipmentEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<ShipmentEvent>> receiveEvent(
            @Valid @RequestBody ShipmentEvent event) {

        return producer.publish(event)
                .thenApply(result -> ResponseEntity.ok(event))
                .exceptionally(error -> {
                    throw new ResponseStatusException(
                            HttpStatus.SERVICE_UNAVAILABLE,
                            "Could not publish shipment event",
                            error
                    );
                });
    }
}