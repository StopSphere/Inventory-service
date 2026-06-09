package com.order_service.shopsphere.order_service.DTO.Event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class InventoryReservedEvent {

    private UUID orderId;
    private UUID productId;
    private Integer quantity;
    private BigDecimal amount;
}
