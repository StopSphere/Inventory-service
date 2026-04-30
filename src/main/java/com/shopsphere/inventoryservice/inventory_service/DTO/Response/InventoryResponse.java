package com.shopsphere.inventoryservice.inventory_service.DTO.Response;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
public class InventoryResponse
{
    private UUID productId;
    private Integer quantity;
}
