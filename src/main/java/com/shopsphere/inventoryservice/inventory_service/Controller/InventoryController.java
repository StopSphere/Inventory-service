package com.shopsphere.inventoryservice.inventory_service.Controller;

import com.shopsphere.inventoryservice.inventory_service.DTO.Request.CreateInventoryRequest;
import com.shopsphere.inventoryservice.inventory_service.DTO.Response.InventoryResponse;
import com.shopsphere.inventoryservice.inventory_service.Service.InventoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/api/inventory")
public class InventoryController {

    private final InventoryService inventoryService;
    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }


    @PostMapping
    public ResponseEntity<InventoryResponse> addStock(@RequestBody CreateInventoryRequest createInventoryRequest ) {
        return ResponseEntity.ok().body(inventoryService.addStock(createInventoryRequest));
    }

    @GetMapping("/{productId}")
    public ResponseEntity<InventoryResponse> getStock(@PathVariable UUID productId) {
        return ResponseEntity.ok().body(inventoryService.getStock(productId));
    }
    @PutMapping("/remove")
    public ResponseEntity<InventoryResponse> removeStock(
            @RequestParam UUID productId,
            @RequestParam int quantity
    ) {
        return ResponseEntity.ok(inventoryService.removeStock(productId, quantity));
    }

    @RequestMapping
    public ResponseEntity<List<InventoryResponse>> getAllInventory() {
        return ResponseEntity.ok(inventoryService.getAllInventory());
    }
}
