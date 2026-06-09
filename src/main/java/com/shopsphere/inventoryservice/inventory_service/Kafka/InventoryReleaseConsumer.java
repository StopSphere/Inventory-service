package com.shopsphere.inventoryservice.inventory_service.Kafka;

import com.shopsphere.payment_Services.Kafka.InventoryReleaseEvent;
import com.shopsphere.inventoryservice.inventory_service.Entity.Inventory;
import com.shopsphere.inventoryservice.inventory_service.Repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class InventoryReleaseConsumer {

    private final InventoryRepository inventoryRepository;

    @KafkaListener(
            topics = "inventory-release",
            groupId = "inventory-service-group",
            properties = {
                    "spring.json.value.default.type=com.shopsphere.payment_Services.Kafka.InventoryReleaseEvent"
            }
    )
    public void consume(InventoryReleaseEvent event) {
        System.out.println("===== RELEASE EVENT =====");
        System.out.println("ORDER ID = " + event.getOrderId());
        System.out.println("PRODUCT ID = " + event.getProductId());
        System.out.println("QUANTITY = " + event.getQuantity());
        Inventory inventory =
                inventoryRepository.findByProductId(
                        event.getProductId()
                ).orElseThrow();

        inventory.setQuantity(
                inventory.getQuantity()
                        + event.getQuantity()
        );

        inventoryRepository.save(inventory);

        System.out.println(
                "Inventory Restored For Product : "
                        + event.getProductId()
        );
    }
}
