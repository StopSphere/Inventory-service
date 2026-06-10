package com.shopsphere.inventoryservice.inventory_service.Entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Entity
@Table(name = "processed_inventory_release")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedInventoryRelease {

    @Id
    private UUID orderId;
}
