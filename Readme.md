# Inventory Service

## Service Overview

The Inventory Service is a core microservice within the ShopSphere e-commerce platform that manages product stock and inventory levels. It operates as an event-driven service using Apache Kafka for asynchronous communication with the Order Service, implementing the **Saga pattern** for distributed transactions.

When an order is created, the Inventory Service receives an `OrderCreatedEvent`, attempts to reserve the requested quantity of inventory, and responds with either an `InventoryReservedEvent` (success) or `InventoryFailedEvent` (failure). This ensures inventory consistency across the distributed system.

---

## Service Responsibilities

- **Stock Management** — Add, retrieve, update, and manage product inventory quantities
- **Inventory Reservation** — Reserve stock when orders are placed via Kafka events
- **Concurrency Control** — Prevent overselling using pessimistic locking on inventory updates
- **Idempotency** — Track processed orders to prevent duplicate inventory deductions
- **Event Publishing** — Publish inventory reservation success/failure events back to Order Service
- **Error Handling** — Implement retry mechanisms and Dead Letter Topic (DLT) for failed events
- **Service Registration** — Register with Eureka Discovery Server for automatic service discovery

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Java 21 | Modern JVM language with latest features |
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

### Event-Driven Architecture

The Inventory Service operates in a request-response pattern through Kafka events, implementing the **Saga Pattern** for distributed transactions:

```
┌─────────────────────────────────────────────────────────────┐
│                      E-COMMERCE SYSTEM                      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Order Service                                                │
├───────────────────────────────────────��─────────────────────┤
│                                                              │
│  1. Order Created                                            │
│     └─► [PUBLISH]                                            │
│         "order-created" Topic                                │
│         {orderId, productId, quantity}                       │
│                           │                                  │
│                           ▼                                  │
│        ┌──────────────────────────────────┐                 │
│        │                                  │                 │
│        │    Apache Kafka Broker (9092)    │                 │
│        │                                  │                 │
│        └──────────────────────────────────┘                 │
│                           │                                  │
└───────────────────────────┼──────────────────────────────────┘
                            │
                            │ [SUBSCRIBE]
                            │
┌───────────────────────────┼──────────────────────────────────┐
│ Inventory Service                                            │
├───────────────────────────┼──────────────────────────────────┤
│                           ▼                                  │
│  2. OrderEventConsumer                                       │
│     └─► consumeOrderCreatedEvent()                           │
│                           │                                  │
│     ┌─────────────────────┴────────────────────┐             │
│     │                                          │             │
│     ▼                                          ▼             │
│  Check Idempotency                     Remove Stock         │
│  (ProcessedOrder table)                 (with Lock)         │
│     │                                          │             │
│     └─▶ [Already Processed?]        [Sufficient Stock?]     │
│         YES ─► Log & Ignore                    │             │
│                                            ┌───┴────┐       │
│                                            │        │       │
│                                          YES     NO │       │
│                                            │        │       │
│                          ┌─────────────────┘        │       │
│                          │                         │       │
│                          ▼                         ▼       │
│            Save ProcessedOrder              Throw Exception │
│                          │                                  │
│     ┌────────────────────┴────────────────┐                │
│     │                                     │                │
│     ▼                                     ▼                │
│ [PUBLISH]                            [PUBLISH]            │
│ "inventory-reserved"                 "inventory-failed"   │
│ {orderId}                            {orderId, reason}    │
│                                                            │
└────────────────────────┬───────────────────────────────────┘
                         │
                         │ [SUBSCRIBE]
                         │
         ┌───────────────┴───────────────┐
         │                               │
         ▼                               ▼
    Order Service            Order Service
    [Success path]           [Failure path]
    Update order status      Rollback order
    (Compensation)           (Saga pattern)
```

### Data Flow Steps

1. **Order Created** — Order Service publishes `OrderCreatedEvent` to `order-created` topic
2. **Event Consumed** — Inventory Service listener (`OrderEventConsumer`) receives the event
3. **Idempotency Check** — Service checks if order was already processed (prevent duplicates)
4. **Lock & Reserve** — Service acquires pessimistic lock and attempts to deduct stock
5. **Event Response**:
   - ✅ **Success** → Publish `InventoryReservedEvent` to `inventory-reserved` topic
   - ❌ **Failure** → Publish `InventoryFailedEvent` to `inventory-failed` topic (Order Service compensates)
6. **Retry & DLT** — Failed messages retry 3 times (2s interval) before being sent to Dead Letter Topic

---

## Key Design Patterns Implemented

### 1. **Idempotent Kafka Consumer**

**Purpose:** Prevent duplicate inventory deductions if the same event is consumed multiple times.

**Implementation:**
- `ProcessedOrder` table stores unique `orderId` values of processed events
- Consumer checks if `orderId` exists before processing: `processedOrderRepository.existsById(event.getOrderId())`
- If duplicate detected, event is logged and ignored
- Ensures **exactly-once processing semantics** despite Kafka's at-least-once delivery guarantee

**Code Reference:**
```java
if(processedOrderRepository.existsById(event.getOrderId())) {
    log.warn("duplicate event ignored: {}", event.getOrderId());
    return;
}
```

---

### 2. **Pessimistic Locking for Concurrency**

**Purpose:** Prevent race conditions and overselling when multiple orders target the same product.

**Implementation:**
- Uses JPA `@Lock(LockModeType.PESSIMISTIC_WRITE)` on `InventoryRepository.findByProductIdWithLock()`
- Database acquires row-level exclusive lock during stock removal
- Transaction blocks concurrent updates, ensuring consistency
- Lock is released when transaction completes

**Code Reference:**
```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM Inventory i WHERE i.productId = :productId")
Optional<Inventory> findByProductIdWithLock(UUID productId);
```

**Scenario:**
- Order 1 & Order 2 both request 5 units of Product A (10 units available)
- Order 1 locks → Reserve 5 units → Unlock
- Order 2 locks → Reserve 5 units → Unlock (safe from overselling)

---

### 3. **Dead Letter Topic (DLT) + Retry Mechanism**

**Purpose:** Handle failed event processing gracefully without losing messages.

**Implementation:**
- Kafka listeners configured with `DefaultErrorHandler` in `KafkaConfig`
- Failed messages retry **3 times** with **2-second fixed backoff** between attempts
- After retries exhausted, messages are automatically published to Dead Letter Topic (`order-created-dlt`)
- Dead Letter Topic can be monitored separately for manual intervention

**Configuration:**
```java
FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3); // 2s interval, 3 retries
DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
return new DefaultErrorHandler(recoverer, fixedBackOff);
```

**Example Scenario:**
- Event fails → Retry #1 (waits 2s) → Fails again → Retry #2 → Fails → Retry #3 → Fails
- After 3 failed attempts, message sent to `order-created-dlt` for analysis

---

### 4. **Saga Pattern (Compensating Transaction)**

**Purpose:** Implement distributed transactions across Order Service and Inventory Service.

**Implementation:**
- **Choreography-based Saga** using event chain
- Order Service initiates transaction by publishing `OrderCreatedEvent`
- Inventory Service attempts reservation; **if failure → publishes `InventoryFailedEvent`**
- Order Service consumes failure event and executes **compensating transaction** (rollback order)

**Flow:**
```
Order Service                  Inventory Service
     │                                │
     ├──► [OrderCreatedEvent]  ───────>
     │                                │
     │                        [Reserve Stock]
     │                                │
     │ ◄───────────────────────┤ [InventoryFailedEvent]
     │                                │
     └──► [Rollback Order]     ───────>
            (Compensation)            ✓ Acknowledge
```

**Benefit:** If inventory cannot be reserved, the entire order is rolled back, maintaining consistency without explicit 2-phase commit.

---

## API Endpoints

| HTTP Method | Endpoint | Description | Access | Request Body |
|-------------|----------|-------------|--------|--------------|
| `POST` | `/v1/api/inventory` | Add stock for a product | Public | `{ productId: UUID, quantity: int }` |
| `GET` | `/v1/api/inventory/{productId}` | Get stock quantity of a product | Public | — |
| `PUT` | `/v1/api/inventory/remove?productId=UUID&quantity=int` | Remove stock (manual deduction) | Public | Query params only |
| `GET` | `/v1/api/inventory` | Get all inventory records | Public | — |

### Request/Response Examples

#### 1. Add Stock
**Request:**
```bash
POST http://localhost:8084/v1/api/inventory
Content-Type: application/json

{
  "productId": "550e8400-e29b-41d4-a716-446655440000",
  "quantity": 100
}
```

**Response (200 OK):**
```json
{
  "productId": "550e8400-e29b-41d4-a716-446655440000",
  "quantity": 100
}
```

---

#### 2. Get Stock by Product ID
**Request:**
```bash
GET http://localhost:8084/v1/api/inventory/550e8400-e29b-41d4-a716-446655440000
```

**Response (200 OK):**
```json
{
  "productId": "550e8400-e29b-41d4-a716-446655440000",
  "quantity": 95
}
```

---

#### 3. Remove Stock Manually
**Request:**
```bash
PUT http://localhost:8084/v1/api/inventory/remove?productId=550e8400-e29b-41d4-a716-446655440000&quantity=5
```

**Response (200 OK):**
```json
{
  "productId": "550e8400-e29b-41d4-a716-446655440000",
  "quantity": 90
}
```

---

#### 4. Get All Inventory
**Request:**
```bash
GET http://localhost:8084/v1/api/inventory
```

**Response (200 OK):**
```json
[
  {
    "productId": "550e8400-e29b-41d4-a716-446655440000",
    "quantity": 90
  },
  {
    "productId": "660e8400-e29b-41d4-a716-446655440001",
    "quantity": 150
  }
]
```

---

## Database Schema

### Table: `inventory`

Stores product stock levels.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `inventory_id` | UUID (BINARY(16)) | PRIMARY KEY | Unique inventory record identifier |
| `product_id` | UUID (BINARY(16)) | NOT NULL | Reference to product |
| `quantity` | INT | NOT NULL, DEFAULT 0 | Current stock quantity |

**SQL Definition:**
```sql
CREATE TABLE inventory (
  inventory_id BINARY(16) PRIMARY KEY,
  product_id BINARY(16) NOT NULL,
  quantity INT NOT NULL DEFAULT 0,
  INDEX idx_product_id (product_id)
);
```

---

### Table: `processed_orders`

Tracks processed order events to ensure idempotency.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `order_id` | UUID (BINARY(16)) | PRIMARY KEY | Order identifier (foreign key concept) |

**SQL Definition:**
```sql
CREATE TABLE processed_orders (
  order_id BINARY(16) PRIMARY KEY
);
```

**Purpose:** 
- When `OrderCreatedEvent` is consumed, the `orderId` is stored here
- On subsequent identical events (retries/duplicates), lookup prevents re-processing
- Ensures **idempotent consumer** behavior

---

## Kafka Topics

| Topic Name | Producer | Consumer | Event Type | Purpose |
|------------|----------|----------|-----------|---------|
| `order-created` | Order Service | Inventory Service | `OrderCreatedEvent` | Trigger inventory reservation when order is created |
| `inventory-reserved` | Inventory Service | Order Service | `InventoryReservedEvent` | Signal successful inventory reservation to complete order |
| `inventory-failed` | Inventory Service | Order Service | `InventoryFailedEvent` | Signal inventory reservation failure to trigger order compensation |
| `order-created-dlt` | DefaultErrorHandler | Monitoring/Ops | Failed `OrderCreatedEvent` | Dead Letter Topic for events that failed after 3 retries |

### Topic Configuration

**Bootstrap Servers:** `localhost:9092`

**Consumer Group:** `inventory-service-group`

**Serialization:**
- Key: String
- Value: JSON (via `JsonDeserializer`)

**Auto-Offset Reset:** `latest` (new consumers start from latest offset)

---

## How to Run

### Prerequisites

1. **Java 21+** — JDK installed and `JAVA_HOME` set
2. **MySQL 8.0+** — MySQL server running on `localhost:3307`
3. **Apache Kafka** — Kafka cluster running on `localhost:9092`
4. **Discovery Server** — Eureka server running on `http://host.docker.internal:8761/eureka/` (or `localhost:8761` for local dev)

### Database Setup

Before running the service, create the MySQL database:

```bash
mysql -u root -p1234
```

```sql
CREATE DATABASE IF NOT EXISTS inventory_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE inventory_db;
```

Hibernate will auto-create tables on first run (configured with `ddl-auto: update`).

### Local Development

#### Step 1: Clone & Navigate
```bash
cd path/to/inventory-service
```

#### Step 2: Start Kafka (if using Docker)
```bash
docker run -d -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.5.0
```

Or use `docker-compose.yml` if available:
```bash
docker-compose up -d
```

#### Step 3: Build Project
```bash
./gradlew build
```

#### Step 4: Run Service
```bash
./gradlew bootRun
```

**Service runs on:** `http://localhost:8084`

#### Step 5: Verify Service Startup

Check Gradle output for:
```
InventoryServiceApplication : Started InventoryServiceApplication in 5.234 seconds
```

Check Eureka registration:
```bash
curl http://localhost:8761/eureka/apps/INVENTORY-SERVICE
```

---

### Docker Deployment

Build Docker image:
```bash
docker build -t inventory-service:latest .
```

Run container:
```bash
docker run -d -p 8084:8084 \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=1234 \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e EUREKA_CLIENT_SERVICE_URL_DEFAULT_ZONE=http://discovery-server:8761/eureka/ \
  --network shopsphere-network \
  inventory-service:latest
```

---

## Environment Variables / Configuration

### application.yml Properties

#### Server Configuration
```yaml
server:
  port: 8084                          # Service port
```

#### Spring Application
```yaml
spring:
  application:
    name: inventory-service           # Service name for Eureka registration
```

#### Kafka Configuration
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092  # Kafka broker address
    
    consumer:
      group-id: inventory-service-group        # Consumer group ID
      auto-offset-reset: latest                # Start from latest if no offset
      key-deserializer: StringDeserializer
      value-deserializer: JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"      # Allow all packages
        spring.json.value.default.type: com.order_service.shopsphere.order_service.DTO.Event.OrderCreatedEvent
    
    producer:
      key-serializer: StringSerializer
      value-serializer: JsonSerializer
```

#### Database Configuration
```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3307/inventory_db?createDatabaseIfNotExist=true
    username: ${DB_USERNAME:root}              # Override with env var
    password: ${DB_PASSWORD:1234}              # Override with env var
    driver-class-name: com.mysql.cj.jdbc.Driver
  
  jpa:
    hibernate:
      ddl-auto: update                         # Auto-create/update schema
    show-sql: true                             # Log SQL queries
    properties:
      hibernate:
        format_sql: true                       # Pretty-print SQL
```

#### Service Discovery
```yaml
eureka:
  client:
    service-url:
      defaultZone: http://host.docker.internal:8761/eureka/  # Eureka server
    fetch-registry: true                       # Fetch service registry
    register-with-eureka: true                 # Register this service
  instance:
    prefer-ip-address: true                    # Use IP instead of hostname
```

### Environment Variables (for Docker/Deployment)

| Variable | Default | Purpose |
|----------|---------|---------|
| `DB_USERNAME` | `root` | MySQL username |
| `DB_PASSWORD` | `1234` | MySQL password |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `EUREKA_CLIENT_SERVICE_URL_DEFAULT_ZONE` | `http://host.docker.internal:8761/eureka/` | Eureka server URL |

---

## Additional Notes

### Logging
- SLF4J is configured via Lombok `@Slf4j`
- All Kafka events are logged with `log.info()` and `log.warn()` for debugging

### Transaction Management
- `@Transactional` annotation on consumer ensures atomic operations
- Database lock held during entire transaction

### Error Handling
- Failed stock deductions throw `RuntimeException` with descriptive messages
- Exceptions are caught in consumer, triggering `InventoryFailedEvent` publication

### Testing
Run all tests:
```bash
./gradlew test
```

---

## Summary

The Inventory Service is a production-ready, event-driven microservice that:
- ✅ Manages product inventory with thread-safe operations
- ✅ Implements distributed transaction patterns (Saga)
- ✅ Ensures data consistency via pessimistic locking
- ✅ Handles failures gracefully with retry & DLT mechanisms
- ✅ Maintains idempotency to prevent duplicate processing
- ✅ Integrates seamlessly with other ShopSphere services via Kafka
