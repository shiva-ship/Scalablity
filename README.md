# Scalability Showcase: FlashSale Inventory Reservation

A Java 21 / Spring Boot system that solves a real industry problem: **flash sales**, where
millions of users compete for a few thousand units of a hot product within seconds.
Think Big Billion Days, Prime Day, concert ticket drops, or Tatkal train booking.

The naive `@Transactional` "read stock → decrement → save" approach oversells, melts the
database, and cascades into an outage. This project rebuilds it step by step with
scalability patterns, and **benchmarks every step**:

- Atomic in-memory reservation (Redis Lua): zero oversell
- Hot-key sharding: one hot SKU spread across the cluster
- Async write path with Kafka: the API is decoupled from DB latency and bursts are absorbed
- Idempotency end to end: safe retries
- Rate limiting, circuit breakers, bulkheads: graceful degradation
- Multi-level caching for read-heavy browse traffic
- Transactional outbox, expiry and reconciliation: self-healing consistency
- Stateless services that scale horizontally, with Prometheus/Grafana showing the effect

See **[docs/DESIGN.md](docs/DESIGN.md)** for the problem analysis, architecture, and phased roadmap.
