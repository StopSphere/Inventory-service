package com.shopsphere.inventoryservice.inventory_service.Service.Impl;

import com.shopsphere.inventoryservice.inventory_service.DTO.Request.CreateInventoryRequest;
import com.shopsphere.inventoryservice.inventory_service.DTO.Response.InventoryResponse;
import com.shopsphere.inventoryservice.inventory_service.Entity.Inventory;
import com.shopsphere.inventoryservice.inventory_service.Repository.InventoryRepository;
import com.shopsphere.inventoryservice.inventory_service.Service.InventoryService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
@Service
@RequiredArgsConstructor
public class InventoryServiceImpl implements InventoryService {
    private final InventoryRepository inventoryRepository;

    @Override
    public InventoryResponse addStock(CreateInventoryRequest request) {
        Inventory inventory = inventoryRepository.findByProductId(request.getProductId())
                .orElse(new Inventory(null , request.getProductId(), 0));
        inventory.setQuantity(inventory.getQuantity() + request.getQuantity());
        inventoryRepository.save(inventory);

        return new InventoryResponse(inventory.getProductId(), inventory.getQuantity());
    }

    @Override
    @Transactional
    public InventoryResponse removeStock(UUID productId, Integer quantity) {

        Inventory inventory = inventoryRepository.findByProductIdWithLock(productId)
                .orElseThrow(() -> new RuntimeException("Product not found"));

        if(quantity < 0) {
            throw new RuntimeException("Quantity must be positive");
        }
        if (inventory.getQuantity() < quantity) {
            throw new RuntimeException("Insufficient stock");
        }

        inventory.setQuantity(inventory.getQuantity() - quantity);

        inventoryRepository.save(inventory);

        return new InventoryResponse(inventory.getProductId(), inventory.getQuantity());
    }

    @Override
    public InventoryResponse getStock(UUID productId) {
        Inventory inventory = inventoryRepository.findByProductId(productId).orElseThrow(() -> new RuntimeException("Product not found in inventory"));
        return new InventoryResponse(inventory.getProductId(), inventory.getQuantity());
    }
    @Override
    public List<InventoryResponse> getAllInventory() {

        return inventoryRepository.findAll()
                .stream()
                .map(inv -> new InventoryResponse(
                        inv.getProductId(),
                        inv.getQuantity()
                ))
                .toList();
    }
}
