package com.order_service.shopsphere.order_service.DTO.Event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class InventoryFailedEvent {

    private UUID orderId;
    private String reason;
}
