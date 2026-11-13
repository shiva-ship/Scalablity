# FlashSale: High-Concurrency Inventory Reservation

## 1. The industry problem

Every big e-commerce and ticketing platform runs into the same failure during a **flash sale**
(Big Billion Days, Prime Day, an iPhone launch, Taylor Swift tickets, IRCTC Tatkal booking):

* **Massive, spiky traffic.** Normal load might be ~500 req/s. When the sale opens it jumps to
  **50k–500k req/s within seconds**, all aimed at a **handful of hot SKUs**.
* **Very little stock.** For example, 10,000 units against 2 million buyers.
* **Hard correctness rules.** Never oversell, never double-charge, and never lose an order the
  user was told succeeded.

The textbook Java/Spring implementation fails under this load:

```java
@Transactional
public Order buy(long skuId, long userId) {
    Product p = repo.findById(skuId);          // every request reads the same row
    if (p.getStock() <= 0) throw new SoldOut();
    p.setStock(p.getStock() - 1);              // race condition -> oversell
    return orderRepo.save(new Order(...));     // slow DB write on the hot path
}
```

| Symptom | Root cause |
|---|---|
| Overselling | Read-modify-write race across threads and JVMs |
| DB CPU at 100%, timeouts | Every request hits one hot row and takes a row lock |
| Thread pool exhaustion → cascading failure | Synchronous, blocking hot path |
| Duplicate orders | Client retries without idempotency |
| Bots grab everything | No admission control or rate limiting |

Adding more app servers doesn't fix any of this. The bottleneck is the **shared state**
(the one inventory row), not compute.

## 2. Goals and non-goals

**Functional**
* Browse sale items (heavy read).
* Reserve (buy) an item: at most N per user and never more than the stock.
* Pay within a time window. If the user doesn't, the reservation expires and the stock goes back.
* Check order status.

**Non-functional (targets we'll benchmark)**
| Metric | Target |
|---|---|
| Reserve API throughput | ≥ 20k req/s on a laptop-scale cluster, scaling linearly with nodes |
| Reserve API p99 latency | < 50 ms |
| Oversell | **0**, verified by an invariant check after each load test |
| Accepted-order loss | **0**, verified across a service crash |
| Availability | Browse stays up even if the order DB is down |

**Non-goals:** a real payment gateway (we'll stub it), a UI, and auth beyond a user-id header/JWT stub.

## 3. Core idea

> **Keep the DB out of the contended path. Make the hot path in-memory and atomic, then
> asynchronous. Shard the hot key. Reject early and cheaply.**

```
 Client
   │
   ▼
┌──────────────────────┐
│ API Gateway          │  rate limit (token bucket in Redis), bot/user caps,
│ (Spring Cloud GW)    │  request-id → idempotency key
└─────────┬────────────┘
          │
   ┌──────┴────────────────────────────┐
   ▼                                   ▼
┌───────────────┐            ┌────────────────────────┐
│ Catalog Svc   │            │ Reservation Svc (N×)   │  stateless, horizontally scaled
│ read-heavy    │            │  1. idempotency check  │
│ Caffeine L1 + │            │  2. Lua: atomic        │
│ Redis L2 +    │            │     decrement stock    │──► Redis Cluster
│ CDN headers   │            │     on shard + record  │    (stock sharded per SKU:
└──────┬────────┘            │     user               │     sku:42:s0 … sku:42:s15)
       │                     │  3. publish event      │
       │                     │  4. return 202+orderId │
       │                     └──────────┬─────────────┘
       │                                │ ReservationCreated
       │                                ▼
       │                      ┌──────────────────┐
       │                      │ Kafka            │  topic partitioned by orderId
       │                      │ (orders.reserved)│
       │                      └────────┬─────────┘
       │                               ▼
       │                     ┌────────────────────┐
       │                     │ Order Svc (M×)     │  consumer group, batch inserts,
       │                     │ idempotent upsert  │  outbox → payment
       │                     └────────┬───────────┘
       ▼                              ▼
   PostgreSQL (catalog)        PostgreSQL (orders; partitioned by created_at,
                               shardable by order_id hash)
                                      │
                      ┌───────────────┴──────────────┐
                      ▼                              ▼
              Payment Svc (stub)            Expiry Worker
              PaymentSucceeded/Failed       unpaid after TTL → release
                                            stock back into Redis shard
```

## 4. Scalability techniques and where each is shown

| # | Technique | Where | Why it matters |
|---|---|---|---|
| 1 | **Stateless services + horizontal scaling** | Reservation / Order svc | Any node serves any request; scale with `docker compose --scale` or a K8s HPA |
| 2 | **Atomic in-memory decrement (Redis Lua)** | Reservation svc | Check-and-decrement in a single atomic op, so no race, no DB lock, ~µs |
| 3 | **Hot-key sharding** | Stock split into K sub-counters per SKU | One Redis key = one CPU core. Sharding spreads a hot SKU across the cluster |
| 4 | **Async write path (CQRS-lite)** | Kafka between reserve and persist | API latency no longer depends on DB latency; Kafka absorbs bursts (load leveling) |
| 5 | **Idempotency keys** | Gateway + Lua + DB unique constraint | Safe retries end to end: no double orders under at-least-once delivery |
| 6 | **Transactional Outbox** | Order svc → Payment | DB write and event publish are atomic without 2PC |
| 7 | **Rate limiting and admission control** | Gateway (Redis token bucket), per-user cap | Shed load early, where rejecting is cheapest; protects downstream |
| 8 | **Multi-level caching** | Catalog: Caffeine → Redis → DB, stampede protection | Browse traffic (~95% of load) never touches the DB |
| 9 | **Backpressure and bulkheads** | Resilience4j circuit breakers, bounded pools, virtual threads (Java 21) | One slow dependency can't take down the whole service |
| 10 | **Partitioning** | Kafka partitions by key; Postgres table partitioning | Ordering per key with parallelism across keys |
| 11 | **Batching** | Order consumer does batch `INSERT ... ON CONFLICT DO NOTHING` | 10–50× fewer DB round trips |
| 12 | **Eventual consistency + reconciliation** | Expiry worker, nightly Redis↔DB reconciler | Self-healing when components crash mid-flow |
| 13 | **Observability** | Micrometer → Prometheus → Grafana, OpenTelemetry traces | You can't scale what you can't measure |
| 14 | **Load testing as a feature** | Gatling/k6 scenarios + oversell invariant checker | Turns "it scales" into numbers |

## 5. The critical piece: atomic reservation

```lua
-- KEYS[1] = stock shard key   e.g. stock:{sku42}:s7
-- KEYS[2] = per-user counter  e.g. bought:{sku42}:user123
-- KEYS[3] = idempotency key   e.g. idem:{req-uuid}
-- ARGV[1] = per-user limit, ARGV[2] = idem TTL seconds, ARGV[3] = orderId
local prev = redis.call('GET', KEYS[3])
if prev then return {2, prev} end                        -- duplicate request: same answer
if tonumber(redis.call('GET', KEYS[2]) or '0') >= tonumber(ARGV[1]) then
  return {3, ''}                                         -- user limit reached
end
local left = redis.call('DECR', KEYS[1])
if left < 0 then redis.call('INCR', KEYS[1]); return {0, ''} end   -- shard empty
redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[2])
return {1, ARGV[3]}                                      -- reserved
```

Hash tags (`{sku42}`) keep these keys in one cluster slot, so the script stays atomic.
**Shard selection:** pick a random shard. If it's empty, try the remaining shards (bounded
attempts), and maintain a "sold-out" bitmap so we stop probing once every shard is empty.
A sold-out SKU is then rejected from local memory (a JVM-level flag that Redis pub/sub
broadcasts to every node) without any network call.

## 6. Failure scenarios we'll demonstrate

| Failure | Behaviour |
|---|---|
| Order svc down | Reservations keep succeeding. Kafka buffers them and the consumer catches up on restart |
| Redis node fails | Replica promotion. Reservations for that slot fail fast (503) while other SKUs are unaffected |
| Event published, then the DB insert crashes | Redelivery + `ON CONFLICT DO NOTHING` = exactly-once effect |
| User never pays | Expiry worker releases the stock back to a shard after the TTL |
| Kafka unavailable | Reservation is compensated (stock re-incremented) and the client gets 503. No phantom orders |

## 7. Tech stack

Java 21 (virtual threads) · Spring Boot 3 · Spring Cloud Gateway · Redis 7 (cluster) ·
Kafka (KRaft) · PostgreSQL 16 · Resilience4j · Micrometer/Prometheus/Grafana ·
Testcontainers · Gatling · Docker Compose (and optional K8s manifests with HPA)

## 8. Repo layout (planned)

```
flashsale/
├── gateway/              # rate limiting, routing
├── catalog-service/      # cached reads
├── reservation-service/  # hot path: Redis Lua + Kafka producer
├── order-service/        # Kafka consumer, Postgres, outbox
├── payment-service/      # stub
├── common/               # events, DTOs
├── load-test/            # Gatling + invariant checker
├── infra/                # docker-compose, grafana dashboards, k8s
└── docs/                 # this file, ADRs, benchmark results
```

## 9. Delivery phases (each one is a runnable, benchmarked milestone)

1. **The naive baseline.** A single Spring Boot app with the `@Transactional` approach above,
   plus a load test that *proves* it oversells and collapses. This is the "before" picture.
2. **Atomic Redis reservation.** Zero oversell. Benchmark again.
3. **Async persistence via Kafka + idempotent consumer.** Latency drops and the service survives a DB outage.
4. **Hot-key sharding + local sold-out cache.** Throughput scales with Redis nodes.
5. **Gateway rate limiting, circuit breakers, bulkheads.** Graceful degradation under 10× overload.
6. **Expiry/reconciliation + outbox + payment stub.** Full lifecycle.
7. **Observability dashboards + horizontal scaling demo.** 1 → 2 → 4 nodes, with a throughput graph.

Each phase records its numbers in `docs/benchmarks.md`, so the repo tells a measurable
before/after story.
