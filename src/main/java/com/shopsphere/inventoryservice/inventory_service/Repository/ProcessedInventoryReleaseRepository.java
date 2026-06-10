package com.shopsphere.inventoryservice.inventory_service.Repository;

import com.shopsphere.inventoryservice.inventory_service.Entity.ProcessedInventoryRelease;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ProcessedInventoryReleaseRepository extends JpaRepository<ProcessedInventoryRelease, UUID> {
}
