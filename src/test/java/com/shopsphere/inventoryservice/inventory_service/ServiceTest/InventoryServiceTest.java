package com.shopsphere.inventoryservice.inventory_service.ServiceTest;

import com.shopsphere.inventoryservice.inventory_service.Entity.Inventory;
import com.shopsphere.inventoryservice.inventory_service.Repository.InventoryRepository;
import com.shopsphere.inventoryservice.inventory_service.Service.Impl.InventoryServiceImpl;
import com.shopsphere.inventoryservice.inventory_service.Service.InventoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class InventoryServiceTest {

    @Mock
    private InventoryRepository inventoryRepository;

    @InjectMocks
    private InventoryServiceImpl inventoryService;

    @Test
    void shouldRemoveStockSuccessfully(){

        UUID productId = UUID.randomUUID();

        Inventory inventory = new Inventory(
                UUID.randomUUID(),
                productId,
                10
        );

        when(
                inventoryRepository.findByProductIdWithLock(productId)
        ).thenReturn(Optional.of(inventory));

        inventoryService.removeStock(productId, 2);

        assertEquals(8, inventory.getQuantity());

        verify(inventoryRepository).save(inventory);
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
    }
}
