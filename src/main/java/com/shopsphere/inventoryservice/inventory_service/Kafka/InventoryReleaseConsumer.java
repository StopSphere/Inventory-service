package com.shopsphere.inventoryservice.inventory_service.Kafka;

import com.shopsphere.inventoryservice.inventory_service.Entity.ProcessedInventoryRelease;
import com.shopsphere.inventoryservice.inventory_service.Repository.ProcessedInventoryReleaseRepository;
import com.shopsphere.payment_Services.Kafka.InventoryReleaseEvent;
import com.shopsphere.inventoryservice.inventory_service.Entity.Inventory;
import com.shopsphere.inventoryservice.inventory_service.Repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
@Service
@RequiredArgsConstructor
public class InventoryReleaseConsumer {

    private final InventoryRepository inventoryRepository;
    private final ProcessedInventoryReleaseRepository processedInventoryReleaseRepository;

    @KafkaListener(
            topics = "inventory-release",
            groupId = "inventory-service-group",
            properties = {
                    "spring.json.value.default.type=com.shopsphere.payment_Services.Kafka.InventoryReleaseEvent"
            }
    )
    @Transactional
    public void consume(InventoryReleaseEvent event) {

        System.out.println("===== RELEASE EVENT =====");
        System.out.println("ORDER ID = " + event.getOrderId());
        System.out.println("PRODUCT ID = " + event.getProductId());
        System.out.println("QUANTITY = " + event.getQuantity());

        // Idempotency Check
        if (processedInventoryReleaseRepository.existsById(event.getOrderId())) {

            System.out.println(
                    "Duplicate release event ignored for order : "
                            + event.getOrderId()
            );

            return;
        }

        Inventory inventory =
                inventoryRepository
                        .findByProductIdWithLock(
                                event.getProductId()
                        )
                        .orElseThrow();

        inventory.setQuantity(
                inventory.getQuantity()
                        + event.getQuantity()
        );

        inventoryRepository.save(inventory);

        processedInventoryReleaseRepository.save(
                new ProcessedInventoryRelease(
                        event.getOrderId()
                )
        );

        System.out.println(
                "Inventory Restored For Product : "
                        + event.getProductId()
        );
    }
}