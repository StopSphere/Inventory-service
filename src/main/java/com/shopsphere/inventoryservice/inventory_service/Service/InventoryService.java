package com.shopsphere.inventoryservice.inventory_service.Service;

import com.shopsphere.inventoryservice.inventory_service.DTO.Request.CreateInventoryRequest;
import com.shopsphere.inventoryservice.inventory_service.DTO.Response.InventoryResponse;

import java.util.List;
import java.util.UUID;

public interface InventoryService
{
    public InventoryResponse addStock(CreateInventoryRequest request);

    public InventoryResponse removeStock(UUID productId, Integer quantity);

    public InventoryResponse getStock(UUID productId);
    public List<InventoryResponse> getAllInventory();
}
