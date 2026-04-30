package com.shopsphere.inventoryservice.inventory_service.DTO.Request;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class CreateInventoryRequest {
    private UUID productId;
    private Integer quantity;
}
