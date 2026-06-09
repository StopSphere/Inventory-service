package com.shopsphere.inventoryservice.inventory_service.Kafka;

import com.order_service.shopsphere.order_service.DTO.Event.InventoryFailedEvent;
import com.order_service.shopsphere.order_service.DTO.Event.InventoryReservedEvent;
import com.order_service.shopsphere.order_service.DTO.Event.OrderCreatedEvent;
import com.shopsphere.inventoryservice.inventory_service.Entity.ProcessedOrder;
import com.shopsphere.inventoryservice.inventory_service.Repository.ProcessedOrderRepository;
import com.shopsphere.inventoryservice.inventory_service.Service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {
    private final InventoryService inventoryService;
    private final InventoryEventProducer inventoryEventProducer;
    private final ProcessedOrderRepository processedOrderRepository;

    @KafkaListener(topics = "order-created", groupId = "inventory-service-group")
    @Transactional
    public void consumeOrderCreatedEvent(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent: {}", event);

        if(processedOrderRepository.existsById(event.getOrderId())) {
            log.warn("duplicate event ignored: {} " ,event.getOrderId() );
            return;
        }
        try {
            inventoryService.removeStock(
                    event.getProductId(),
                    event.getQuantity()
            );
            processedOrderRepository.save(
                    new ProcessedOrder(event.getOrderId())
            );

            InventoryReservedEvent reservedEvent =
                    new InventoryReservedEvent(event.getOrderId(),
                            event.getProductId(),
                            event.getQuantity() ,
                            event.getAmount()
                            );

            inventoryEventProducer.publishInventoryReservedEvent(reservedEvent);
            log.info("Inventory reserved for order: {}", event.getOrderId());

        } catch (Exception ex) {
            log.error("Failed to reserve inventory for order {}: {}",
                event.getOrderId(), ex.getMessage());

            InventoryFailedEvent failedEvent =
                    new InventoryFailedEvent(
                            event.getOrderId(),
                            ex.getMessage()
                    );

            inventoryEventProducer.publishInventoryFailedEvent(failedEvent);
        }
    }
}