package com.shopsphere.inventoryservice.inventory_service.Repository;

import com.shopsphere.inventoryservice.inventory_service.Entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface InventoryRepository extends JpaRepository<Inventory, UUID>
{


    Optional<Inventory> findByProductId(UUID productId);
}
