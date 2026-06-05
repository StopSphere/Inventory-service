package com.shopsphere.inventoryservice.inventory_service.Kafka;

import com.order_service.shopsphere.order_service.DTO.Event.InventoryFailedEvent;
import com.order_service.shopsphere.order_service.DTO.Event.InventoryReservedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class InventoryEventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishInventoryReservedEvent(InventoryReservedEvent inventoryReservedEvent) {
        kafkaTemplate.send("inventory-reserved",
            inventoryReservedEvent.getOrderId().toString(),
            inventoryReservedEvent);
    }

    public void publishInventoryFailedEvent(InventoryFailedEvent event) {
        kafkaTemplate.send("inventory-failed",
            event.getOrderId().toString(),
            event);
    }
}
