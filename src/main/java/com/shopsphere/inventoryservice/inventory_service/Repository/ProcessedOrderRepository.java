package com.shopsphere.inventoryservice.inventory_service.Repository;

import com.shopsphere.inventoryservice.inventory_service.Entity.ProcessedOrder;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, UUID> {
}
