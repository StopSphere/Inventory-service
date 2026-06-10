package com.shopsphere.inventoryservice.inventory_service.Entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Entity
@Table(name="inventory")
@Getter
@Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class Inventory {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID )
    private UUID inventoryId;

    @Column(unique = true)
    private UUID productId;

    private Integer quantity;
}
