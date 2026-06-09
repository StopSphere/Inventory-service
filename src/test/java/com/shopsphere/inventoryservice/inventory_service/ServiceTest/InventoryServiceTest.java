package com.shopsphere.inventoryservice.inventory_service.ServiceTest;

import com.shopsphere.inventoryservice.inventory_service.DTO.Request.CreateInventoryRequest;
import com.shopsphere.inventoryservice.inventory_service.DTO.Response.InventoryResponse;
import com.shopsphere.inventoryservice.inventory_service.Entity.Inventory;
import com.shopsphere.inventoryservice.inventory_service.Repository.InventoryRepository;
import com.shopsphere.inventoryservice.inventory_service.Service.Impl.InventoryServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private InventoryRepository inventoryRepository;

    @InjectMocks
    private InventoryServiceImpl inventoryService;

    @Test
    void shouldRemoveStockSuccessfully() {

        UUID productId = UUID.randomUUID();

        Inventory inventory = new Inventory(
                UUID.randomUUID(),
                productId,
                10
        );

        when(
                inventoryRepository.findByProductIdWithLock(productId)
        ).thenReturn(Optional.of(inventory));

        InventoryResponse response =
                inventoryService.removeStock(productId, 2);

        assertEquals(8, inventory.getQuantity());
        assertEquals(8, response.getQuantity());

        verify(inventoryRepository, times(1))
                .save(inventory);
    }

    @Test
    void shouldThrowExceptionWhenStockIsInsufficient() {

        UUID productId = UUID.randomUUID();

        Inventory inventory = new Inventory(
                UUID.randomUUID(),
                productId,
                2
        );

        when(
                inventoryRepository.findByProductIdWithLock(productId)
        ).thenReturn(Optional.of(inventory));

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> inventoryService.removeStock(productId, 5)
        );

        assertEquals(
                "Insufficient stock",
                exception.getMessage()
        );

        verify(inventoryRepository, never())
                .save(any());
    }

    @Test
    void shouldThrowExceptionWhenProductNotFound() {

        UUID productId = UUID.randomUUID();

        when(
                inventoryRepository.findByProductIdWithLock(productId)
        ).thenReturn(Optional.empty());

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> inventoryService.removeStock(productId, 2)
        );

        assertEquals(
                "Product not found",
                exception.getMessage()
        );

        verify(inventoryRepository, never())
                .save(any());
    }

    @Test
    void shouldAddStockToExistingInventory() {

        UUID productId = UUID.randomUUID();

        CreateInventoryRequest request =
                new CreateInventoryRequest();

        request.setProductId(productId);
        request.setQuantity(5);

        Inventory inventory =
                new Inventory(
                        UUID.randomUUID(),
                        productId,
                        10
                );

        when(
                inventoryRepository.findByProductId(productId)
        ).thenReturn(Optional.of(inventory));

        InventoryResponse response =
                inventoryService.addStock(request);

        assertEquals(15, inventory.getQuantity());
        assertEquals(15, response.getQuantity());

        verify(inventoryRepository, times(1))
                .save(inventory);
    }

    @Test
    void shouldCreateInventoryWhenProductDoesNotExist() {

        UUID productId = UUID.randomUUID();

        CreateInventoryRequest request =
                new CreateInventoryRequest();

        request.setProductId(productId);
        request.setQuantity(5);

        when(
                inventoryRepository.findByProductId(productId)
        ).thenReturn(Optional.empty());

        InventoryResponse response =
                inventoryService.addStock(request);

        assertEquals(productId, response.getProductId());
        assertEquals(5, response.getQuantity());

        verify(inventoryRepository, times(1))
                .save(any(Inventory.class));
    }

    @Test
    void shouldReturnStockSuccessfully() {

        UUID productId = UUID.randomUUID();

        Inventory inventory =
                new Inventory(
                        UUID.randomUUID(),
                        productId,
                        20
                );

        when(
                inventoryRepository.findByProductId(productId)
        ).thenReturn(Optional.of(inventory));

        InventoryResponse response =
                inventoryService.getStock(productId);

        assertEquals(productId,
                response.getProductId());

        assertEquals(20,
                response.getQuantity());
    }

    @Test
    void shouldThrowExceptionWhenGettingStockForUnknownProduct() {

        UUID productId = UUID.randomUUID();

        when(
                inventoryRepository.findByProductId(productId)
        ).thenReturn(Optional.empty());

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () -> inventoryService.getStock(productId)
                );

        assertEquals(
                "Product not found in inventory",
                exception.getMessage()
        );
    }

    @Test
    void shouldReturnAllInventory() {

        Inventory inv1 =
                new Inventory(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        10
                );

        Inventory inv2 =
                new Inventory(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        20
                );

        when(
                inventoryRepository.findAll()
        ).thenReturn(List.of(inv1, inv2));

        List<InventoryResponse> response =
                inventoryService.getAllInventory();

        assertEquals(2, response.size());

        assertEquals(10,
                response.get(0).getQuantity());

        assertEquals(20,
                response.get(1).getQuantity());
    }
}