package com.shopsphere.inventoryservice.inventory_service.Entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "processed_orders")
@Data
public class ProcessedOrder {
    @Id
    private UUID orderId;
}
