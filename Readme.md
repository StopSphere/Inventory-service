# Inventory Service

## Service Overview

The Inventory Service is a core microservice within the ShopSphere e-commerce platform that manages product stock and inventory levels. It operates as an event-driven service using Apache Kafka for asynchronous communication, implementing the **Choreography-based Saga pattern** for distributed transactions across three services: Order Service, Inventory Service, and Payment Service.

When an order is created, the Inventory Service receives an `OrderCreatedEvent`, attempts to reserve the requested quantity, and responds with either an `InventoryReservedEvent` (success) or `InventoryFailedEvent` (failure). If payment subsequently fails, the Payment Service triggers a compensation event and inventory is automatically restored.

---

## Service Responsibilities

- **Stock Management** — Add, retrieve, update, and manage product inventory quantities
- **Inventory Reservation** — Reserve stock when orders are placed via Kafka events
- **Concurrency Control** — Prevent overselling using pessimistic locking on inventory updates
- **Idempotency** — Track processed orders to prevent duplicate inventory deductions
- **Event Publishing** — Publish inventory reservation success/failure events
- **Saga Compensation** — Restore inventory when Payment Service reports payment failure
- **Error Handling** — Implement retry mechanisms and Dead Letter Topic (DLT) for failed events
- **Service Registration** — Register with Eureka Discovery Server for service discovery

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Java 21 | Modern JVM language |
| **Framework** | Spring Boot 4.0.6 | Microservice framework |
| **Data Access** | Spring Data JPA | Object-relational mapping |
| **Database** | MySQL 8.x | Persistent data storage |
| **Message Queue** | Apache Kafka | Asynchronous event streaming |
| **Service Discovery** | Eureka Client | Service registration & discovery |
| **Build Tool** | Gradle | Project build automation |
| **Concurrency** | JPA Pessimistic Locking | Database-level lock mechanism |
| **Lombok** | Code generation | Reduce boilerplate code |

---

## Architecture & Flow

### Complete Saga Flow (Order → Inventory → Payment)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           SHOPSPHERE SAGA FLOW                           │
└──────────────────────────────────────────────────────────────────────────┘

  Order Service                Inventory Service              Payment Service
       │                              │                              │
       │  [1] OrderCreatedEvent       │                              │
       │─────────────────────────────>│                              │
       │   (order-created topic)      │                              │
       │                              │                              │
       │                     [2] Check Idempotency                   │
       │                     [3] Pessimistic Lock                    │
       │                     [4] Reserve Stock                       │
       │                              │                              │
       │                   ┌──────────┴──────────┐                  │
       │               Stock OK?             Stock LOW?             │
       │                   │                     │                  │
       │        [5a] InventoryReservedEvent  [5b] InventoryFailedEvent
       │                   │─────────────────────────────────────>  │
       │                   │   (inventory-reserved topic)    │      │
       │                   │                     │           │      │
       │◄──────────────────────────────────────  │           │      │
       │  [5b] Update Order → CANCELLED          │           │      │
       │       (inventory-failed topic)          │           │      │
       │                                         │    [6] Process   │
       │                                         │       Payment    │
       │                                         │           │      │
       │                                         │    ┌──────┴──────┐
       │                                         │  SUCCESS?    FAILED?
       │                                         │    │              │
       │◄────────────────────────────────────────────-│              │
       │  [7a] Update Order → CONFIRMED          │    │              │
       │                                         │    │   [7b] InventoryReleaseEvent
       │                                         │◄───────────────────
       │                                         │  (inventory-release topic)
       │                                         │              │
       │                                [8] Restore Stock        │
       │                                (Saga Compensation)      │
       │◄────────────────────────────────────────│              │
       │  Update Order → CANCELLED               │              │
       │                                         │              │

─────────────────────────────────────────────────────────────────────────
  HAPPY PATH:   Order Created → Stock Reserved → Payment Success → CONFIRMED
  SAGA COMP 1:  Order Created → Stock Failed   → Order CANCELLED
  SAGA COMP 2:  Order Created → Stock Reserved → Payment Failed → Stock Restored → Order CANCELLED
─────────────────────────────────────────────────────────────────────────
```

### Inventory Service Internal Flow

```
  OrderCreatedEvent received
           │
           ▼
  Already in ProcessedOrder table?
     YES ──► Log & Ignore (Idempotency)
     NO  ──► Continue
           │
           ▼
  Acquire Pessimistic Lock on product row
           │
           ▼
  Sufficient stock?
     NO  ──► Publish InventoryFailedEvent
     YES ──►
           │
           ▼
  Deduct stock + Save ProcessedOrder
           │
           ▼
  Publish InventoryReservedEvent
           │
           ▼
  Release Lock (transaction commit)
```

---

## Key Design Patterns

### 1. Idempotent Kafka Consumer

Prevents duplicate inventory deductions if the same event is consumed multiple times (Kafka at-least-once delivery).

- `ProcessedOrder` table stores `orderId` of every processed event
- On each event: check table first → if exists, skip; if not, process and save

```java
if(processedOrderRepository.existsById(event.getOrderId())) {
    log.warn("duplicate event ignored: {}", event.getOrderId());
    return;
}
```

---

### 2. Pessimistic Locking for Concurrency

Prevents race conditions and overselling when multiple orders target the same product simultaneously.

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM Inventory i WHERE i.productId = :productId")
Optional<Inventory> findByProductIdWithLock(UUID productId);
```

- Order 1 & Order 2 both request 5 units from 10 available
- Order 1 acquires lock → deducts 5 → releases
- Order 2 acquires lock → deducts 5 → releases
- No overselling possible

---

### 3. Dead Letter Topic (DLT) + Retry

Failed Kafka messages retry 3 times with 2-second backoff before being routed to a Dead Letter Topic.

```java
FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3);
DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
return new DefaultErrorHandler(recoverer, fixedBackOff);
```

Flow: `Fail → Retry #1 (2s) → Retry #2 (2s) → Retry #3 (2s) → DLT (order-created-dlt)`

---

### 4. Saga Compensation (Payment Failure)

When payment fails, Payment Service publishes `InventoryReleaseEvent`. Inventory Service consumes it and restores stock.

```java
@KafkaListener(topics = "inventory-release", groupId = "inventory-service-group")
public void consume(InventoryReleaseEvent event) {
    Inventory inventory = inventoryRepository.findByProductId(event.getProductId()).orElseThrow();
    inventory.setQuantity(inventory.getQuantity() + event.getQuantity());
    inventoryRepository.save(inventory);
    log.info("Inventory restored for product: {}", event.getProductId());
}
```

**Compensation scenario:**
```
Initial stock: 100 units
Order placed:  100 - 10 = 90 units  (reserved)
Payment fails: 90  + 10 = 100 units (restored)
Final state:   100 units ✅ System consistent
```

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `order-created` | Order Service | Inventory Service | Trigger inventory reservation |
| `inventory-reserved` | Inventory Service | Order Service, Payment Service | Signal successful reservation |
| `inventory-failed` | Inventory Service | Order Service | Signal failure → Order cancellation |
| `inventory-release` | Payment Service | Inventory Service | Restore stock on payment failure |
| `order-created-dlt` | DefaultErrorHandler | Monitoring | Dead letter for failed events |

**Consumer Group:** `inventory-service-group`  
**Bootstrap Servers:** `localhost:9092`

---

## API Endpoints

| Method | Endpoint | Description | Access |
|--------|----------|-------------|--------|
| `POST` | `/v1/api/inventory` | Add stock for a product | Public |
| `GET` | `/v1/api/inventory/{productId}` | Get stock by product ID | Public |
| `PUT` | `/v1/api/inventory/remove?productId=UUID&quantity=int` | Manually remove stock | Public |
| `GET` | `/v1/api/inventory` | Get all inventory records | Public |

---

## Database Schema

### `inventory` table

| Column | Type | Constraints |
|--------|------|-------------|
| `inventory_id` | UUID | PRIMARY KEY |
| `product_id` | UUID | NOT NULL |
| `quantity` | INT | NOT NULL, DEFAULT 0 |

### `processed_orders` table

| Column | Type | Constraints |
|--------|------|-------------|
| `order_id` | UUID | PRIMARY KEY |

`processed_orders` is the idempotency store — every consumed `OrderCreatedEvent` writes here.

---

## How to Run

### Prerequisites
- Java 21+
- MySQL 8.0+ on `localhost:3307`
- Apache Kafka on `localhost:9092`
- Eureka Discovery Server running

### Steps

```bash
# Create database
mysql -u root -p1234 -e "CREATE DATABASE IF NOT EXISTS inventory_db;"

# Build and run
./gradlew bootRun
```

Service runs on: `http://localhost:8084`

### Docker

```bash
docker build -t inventory-service:latest .

docker run -d -p 8084:8084 \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=1234 \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e EUREKA_CLIENT_SERVICE_URL_DEFAULT_ZONE=http://discovery-server:8761/eureka/ \
  --network shopsphere-network \
  inventory-service:latest
```

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DB_USERNAME` | `root` | MySQL username |
| `DB_PASSWORD` | `1234` | MySQL password |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `EUREKA_CLIENT_SERVICE_URL_DEFAULT_ZONE` | `http://host.docker.internal:8761/eureka/` | Eureka URL |

---

## Summary

| Feature | Status |
|---------|--------|
| Stock management (CRUD) | ✅ |
| Kafka-based inventory reservation | ✅ |
| Idempotent consumer (ProcessedOrder table) | ✅ |
| Pessimistic locking (concurrency control) | ✅ |
| DLT + Retry mechanism | ✅ |
| Saga compensation (payment failure → stock restore) | ✅ |
| Eureka service registration | ✅ |
| Docker support | ✅ |