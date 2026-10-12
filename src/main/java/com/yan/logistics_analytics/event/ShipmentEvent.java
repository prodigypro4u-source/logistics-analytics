package com.yan.logistics_analytics.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

public record ShipmentEvent(
        @NotBlank String eventId,
        @NotBlank String shipmentId,
        @NotBlank String status,
        @NotBlank String location,
        @NotNull Instant eventTime
) {
}