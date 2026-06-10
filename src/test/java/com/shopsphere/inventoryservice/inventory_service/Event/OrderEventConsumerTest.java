package com.shopsphere.inventoryservice.inventory_service.Event;

import com.order_service.shopsphere.order_service.DTO.Event.InventoryFailedEvent;
import com.order_service.shopsphere.order_service.DTO.Event.InventoryReservedEvent;
import com.order_service.shopsphere.order_service.DTO.Event.OrderCreatedEvent;
import com.shopsphere.inventoryservice.inventory_service.Entity.ProcessedOrder;
import com.shopsphere.inventoryservice.inventory_service.Kafka.InventoryEventProducer;
import com.shopsphere.inventoryservice.inventory_service.Kafka.OrderEventConsumer;
import com.shopsphere.inventoryservice.inventory_service.Repository.ProcessedOrderRepository;
import com.shopsphere.inventoryservice.inventory_service.Service.InventoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
@ExtendWith(MockitoExtension.class)
class OrderEventConsumerTest {

    @Mock
    private InventoryService inventoryService;

    @Mock
    private InventoryEventProducer inventoryEventProducer;

    @Mock
    private ProcessedOrderRepository processedOrderRepository;

    @InjectMocks
    private OrderEventConsumer orderEventConsumer;

    @Captor
    private ArgumentCaptor<InventoryReservedEvent> reservedCaptor;

    @Captor
    private ArgumentCaptor<InventoryFailedEvent> failedCaptor;

    @Captor
    private ArgumentCaptor<ProcessedOrder> processedOrderCaptor;

    @Test
    void shouldReserveInventorySuccessfully() {

        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        OrderCreatedEvent event =
                new OrderCreatedEvent(
                        orderId,
                        productId,
                        2,
                        BigDecimal.valueOf(1000)
                );

        when(processedOrderRepository.existsById(orderId))
                .thenReturn(false);

        orderEventConsumer.consumeOrderCreatedEvent(event);

        verify(inventoryService)
                .removeStock(productId, 2);

        verify(processedOrderRepository)
                .save(processedOrderCaptor.capture());

        assertEquals(
                orderId,
                processedOrderCaptor.getValue().getOrderId()
        );

        verify(inventoryEventProducer)
                .publishInventoryReservedEvent(
                        reservedCaptor.capture()
                );

        InventoryReservedEvent reservedEvent =
                reservedCaptor.getValue();

        assertEquals(orderId, reservedEvent.getOrderId());
        assertEquals(productId, reservedEvent.getProductId());
        assertEquals(2, reservedEvent.getQuantity());
        assertEquals(
                BigDecimal.valueOf(1000),
                reservedEvent.getAmount()
        );
    }

    @Test
    void shouldIgnoreDuplicateEvent() {

        UUID orderId = UUID.randomUUID();

        OrderCreatedEvent event =
                new OrderCreatedEvent(
                        orderId,
                        UUID.randomUUID(),
                        2,
                        BigDecimal.valueOf(1000)
                );

        when(processedOrderRepository.existsById(orderId))
                .thenReturn(true);

        orderEventConsumer.consumeOrderCreatedEvent(event);

        verifyNoInteractions(inventoryService);

        verify(processedOrderRepository, never())
                .save(any());

        verify(inventoryEventProducer, never())
                .publishInventoryReservedEvent(any());

        verify(inventoryEventProducer, never())
                .publishInventoryFailedEvent(any());
    }

    @Test
    void shouldPublishInventoryFailedEventWhenStockUnavailable() {

        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        OrderCreatedEvent event =
                new OrderCreatedEvent(
                        orderId,
                        productId,
                        2,
                        BigDecimal.valueOf(1000)
                );

        when(processedOrderRepository.existsById(orderId))
                .thenReturn(false);

        doThrow(
                new RuntimeException("Insufficient stock")
        )
                .when(inventoryService)
                .removeStock(productId, 2);

        orderEventConsumer.consumeOrderCreatedEvent(event);

        verify(processedOrderRepository, never())
                .save(any());

        verify(inventoryEventProducer)
                .publishInventoryFailedEvent(
                        failedCaptor.capture()
                );

        InventoryFailedEvent failedEvent =
                failedCaptor.getValue();

        assertEquals(
                orderId,
                failedEvent.getOrderId()
        );

        assertEquals(
                "Insufficient stock",
                failedEvent.getReason()
        );
    }

    @Test
    void shouldNotPublishReservedEventWhenInventoryFails() {

        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        OrderCreatedEvent event =
                new OrderCreatedEvent(
                        orderId,
                        productId,
                        2,
                        BigDecimal.valueOf(1000)
                );

        when(processedOrderRepository.existsById(orderId))
                .thenReturn(false);

        doThrow(
                new RuntimeException("Inventory error")
        )
                .when(inventoryService)
                .removeStock(productId, 2);

        orderEventConsumer.consumeOrderCreatedEvent(event);

        verify(inventoryEventProducer, never())
                .publishInventoryReservedEvent(any());
    }
}