package com.shopsphere.payment_Services.Kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InventoryReleaseEvent {
    private UUID orderId;
    private UUID productId;
    private Integer quantity;
}
