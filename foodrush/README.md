# 🍔 FoodRush

A production-grade food delivery platform built as a learning project to master **Node.js**, **Docker**, **Apache Kafka**, **PostgreSQL**, and **Elasticsearch**.

This is a fully event-driven microservices system — the same architectural pattern used by Swiggy, Zomato, and DoorDash at scale.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Runtime | Node.js 20 + Express | All microservices |
| Containerization | Docker + Docker Compose | Run the full stack with one command |
| Primary Database | PostgreSQL 16 | Persistent data per service |
| Message Broker | Apache Kafka | Async event streaming between services |
| Search Engine | Elasticsearch 8 | Restaurant & food full-text + geo search |
| Cache | Redis 7 | Sessions, rate limiting, refresh tokens |
| Reverse Proxy | Nginx | SSL termination, load balancing |
| Monitoring | Prometheus + Grafana | Metrics and dashboards |
| CI/CD | GitHub Actions | Lint → test → build → deploy |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Client (Web/Mobile)                │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│            API Gateway :3000  (auth, rate limit)        │
└──┬──────────┬──────────┬──────────┬──────────┬──────────┘
   │          │          │          │          │
┌──▼──┐  ┌───▼───┐  ┌───▼───┐  ┌───▼───┐  ┌───▼────┐
│User │  │ Rest. │  │Order  │  │Deliv. │  │Notif.  │
│:3001│  │ :3002 │  │ :3003 │  │ :3004 │  │(no HTTP│
└──┬──┘  └───┬───┘  └───┬───┘  └───┬───┘  └───┬────┘
   │          │          │          │          │
   └──────────┴──────────┴──────────┴──────────┘
                         │
          ┌──────────────▼──────────────┐
          │     Apache Kafka (events)   │
          └──────────────┬──────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼────┐     ┌─────▼────┐    ┌──────▼───┐
   │Postgres │     │  Redis   │    │  Elastic │
   │(per svc)│     │  Cache   │    │  Search  │
   └─────────┘     └──────────┘    └──────────┘
```

**Key design decisions:**
- **Database per service** — each microservice owns its own PostgreSQL database; no shared tables
- **Event-driven** — services communicate via Kafka topics, not synchronous HTTP calls
- **Saga pattern** — the order lifecycle (`pending → confirmed → preparing → ready → picked_up → delivered`) is driven by events
- **CQRS for search** — restaurant data is written to PostgreSQL and asynchronously indexed into Elasticsearch for reads

---

## Services

### `api-gateway` — Port 3000
The single entry point for all clients. Handles JWT validation, rate limiting (100 req/15 min per IP via Redis), and proxies requests to the correct downstream service. Clients never talk to services directly.

### `user-service` — Port 3001
Registration, login, profile management. Issues short-lived JWT access tokens (15 min) and long-lived refresh tokens stored in Redis. Publishes `user.registered` events to Kafka on signup.

### `restaurant-service` — Port 3002
Restaurant and menu management. On create/update, syncs data to Elasticsearch for search. Exposes a `/api/search` endpoint that performs geo-distance + full-text queries against Elasticsearch.

### `order-service` — Port 3003
The core service. Implements the order state machine with guarded transitions. Every state change publishes an event to Kafka, which triggers delivery assignment, payments, and notifications downstream.

### `delivery-service` — Port 3004
Consumes `order.created` events from Kafka, assigns the nearest available driver using Elasticsearch geo-queries, and tracks real-time GPS position.

### `notification-service` — No HTTP port
A pure Kafka consumer. No REST API. Listens to order/delivery events and sends emails (Nodemailer), SMS, or push notifications. Completely decoupled — if it crashes, orders still work.

---

## Kafka Topics

| Topic | Producer | Consumers | Payload |
|---|---|---|---|
| `order.created` | order-service | delivery-service, notification-service | orderId, userId, items, total |
| `order.status.updated` | order-service | notification-service | orderId, previousStatus, status |
| `order.cancelled` | order-service | delivery-service, notification-service | orderId, reason |
| `delivery.assigned` | delivery-service | order-service, notification-service | orderId, driverId, eta |
| `delivery.status.updated` | delivery-service | order-service, notification-service | orderId, status, location |
| `payment.processed` | payment-service | order-service, notification-service | orderId, amount, status |
| `restaurant.indexed` | restaurant-service | (Elasticsearch consumer) | full restaurant document |
| `notification.send` | any service | notification-service | channel, recipient, template, data |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 24+
- [Node.js](https://nodejs.org/) 20+
- [Git](https://git-scm.com/)

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-username/foodrush.git
cd foodrush
```

### 2. Configure environment

```bash
cp .env.example .env
```

Open `.env` and set your secrets:

```env
# Postgres passwords (change these!)
POSTGRES_USERS_PASSWORD=users_secret_123
POSTGRES_ORDERS_PASSWORD=orders_secret_123
POSTGRES_RESTAURANTS_PASSWORD=restaurants_secret_123

# JWT (use a long random string)
JWT_SECRET=your_super_secret_jwt_key_change_this

# Email (use Mailtrap for development: https://mailtrap.io)
SMTP_HOST=sandbox.smtp.mailtrap.io
SMTP_USER=your_mailtrap_user
SMTP_PASS=your_mailtrap_pass
```

### 3. Install dependencies

```bash
npm install
```

### 4. Start the full stack

```bash
docker-compose up -d
```

This starts: Zookeeper, Kafka, Kafka UI, PostgreSQL (×3), Elasticsearch, Kibana, Redis, Nginx, and all 6 application services.

### 5. Wait for services to be healthy

```bash
# Watch until all services show "healthy" or "running"
docker-compose ps

# Check logs for any service
docker-compose logs -f order-service
```

Kafka and Elasticsearch take ~30–60 seconds to be fully ready on first boot.

### 6. Initialize Elasticsearch index

```bash
curl -X POST http://localhost:3000/api/admin/search/init
```

### 7. Seed sample data

```bash
npm run seed
```

This creates sample restaurants, menus, and a test user (`test@foodrush.app` / `password123`).

---

## Usage

### Register and get a token

```bash
curl -X POST http://localhost:3000/api/users/register \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"password123","name":"Your Name","phone":"+91-9999999999"}'
```

### Search restaurants near you

```bash
curl "http://localhost:3000/api/search?query=biryani&lat=28.6139&lng=77.2090&radius=5"
```

### Place an order

```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "restaurantId": "RESTAURANT_ID",
    "items": [{"menuItemId": "ITEM_ID", "name": "Chicken Biryani", "price": 280, "quantity": 2}],
    "deliveryAddress": {"lat": 28.6139, "lng": 77.2090, "text": "Connaught Place, New Delhi"}
  }'
```

### Watch events flow through Kafka

Open [http://localhost:8090](http://localhost:8090) — the Kafka UI. Navigate to **Topics** and watch messages appear in real time as you place orders.

---

## Development

### Hot reload (dev mode)

```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

Services use `nodemon` in dev mode, so code changes restart the container automatically.

### Run a single service locally (outside Docker)

```bash
cd services/order-service
DATABASE_URL=postgresql://order_svc:password@localhost:5433/orders_db \
KAFKA_BROKERS=localhost:9092 \
node src/index.js
```

### Add a new migration

```bash
cd services/user-service
# Create migration file
echo "ALTER TABLE users ADD COLUMN avatar_url TEXT;" > migrations/003_add_avatar.sql
# Apply it
docker-compose exec postgres-users psql -U user_svc -d users_db -f /migrations/003_add_avatar.sql
```

---

## Testing

```bash
# All tests
npm test

# Unit tests only (fast, no infra needed)
npm run test:unit

# Integration tests (needs Docker services running)
npm run test:integration

# A single service
cd services/order-service && npm test

# With coverage report
npm run test:coverage
```

Tests are written with **Jest** + **Supertest** for HTTP assertions. Integration tests spin up the service against a dedicated test database.

---

## Monitoring

| Tool | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8090 | None |
| Kibana (ES) | http://localhost:5601 | None |
| Grafana | http://localhost:3010 | admin / admin |
| Prometheus | http://localhost:9090 | None |

### Grafana dashboards

Import the pre-built dashboards from `infra/grafana/dashboards/`:
- **FoodRush Overview** — request rates, error rates, latency p99
- **Kafka Consumer Lag** — how far behind each consumer group is
- **PostgreSQL** — query time, connection pool, slow queries
- **Elasticsearch** — indexing rate, search latency, JVM heap

---

## Project Structure

```
foodrush/
├── services/
│   ├── api-gateway/
│   │   ├── src/
│   │   │   └── index.js          # Rate limiting, auth, proxying
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── user-service/
│   │   ├── src/
│   │   │   ├── index.js
│   │   │   ├── routes/
│   │   │   ├── middleware/
│   │   │   └── kafka/
│   │   ├── migrations/
│   │   │   └── init.sql
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── restaurant-service/
│   │   ├── src/
│   │   │   ├── index.js
│   │   │   └── search/
│   │   │       └── elasticsearch.js  # Index mapping, geo queries
│   │   └── ...
│   ├── order-service/
│   │   ├── src/
│   │   │   ├── index.js
│   │   │   └── stateMachine.js   # Order lifecycle transitions
│   │   └── ...
│   ├── delivery-service/
│   └── notification-service/     # Pure Kafka consumer, no HTTP
│       └── src/
│           └── index.js
├── packages/
│   └── shared/
│       └── src/
│           ├── kafka/
│           │   ├── client.js     # Reusable producer/consumer factory
│           │   └── topics.js     # All topic name constants
│           └── logger.js         # Structured JSON logger (pino)
├── infra/
│   ├── nginx/
│   │   └── nginx.conf
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── dashboards/
├── scripts/
│   └── seed.js                   # Sample data generator
├── .github/
│   └── workflows/
│       ├── ci.yml                # Test on every PR
│       └── deploy.yml            # Deploy on merge to main
├── docker-compose.yml
├── docker-compose.dev.yml
├── .env.example
└── package.json                  # Workspace root
```

---

## Learning Path

If you're using this project to learn, work through it in this order:

1. **`docker-compose up`** — understand what each container is, how health checks work, how Docker networking connects them
2. **User Service** — Node.js + Express + PostgreSQL. Learn connection pools, parameterized queries, bcrypt, JWT
3. **Kafka first message** — produce an event from user-service when a user registers. Consume it in notification-service. Understand topics, partitions, and consumer groups
4. **Order Service** — state machines, database transactions, the Saga pattern
5. **Delivery Service consumer** — Kafka consumer groups, offset management, idempotency
6. **Elasticsearch search** — index mappings, full-text analysis, geo-distance queries, nested documents
7. **API Gateway** — reverse proxying, centralized auth, Redis-backed rate limiting
8. **Testing** — Jest unit tests, Supertest integration tests, mocking Kafka
9. **Monitoring** — instrument services with `prom-client`, build a Grafana dashboard
10. **CI/CD** — GitHub Actions pipeline: lint → test → Docker build → push

---

## Common Issues

**Kafka keeps restarting**
Zookeeper must be healthy before Kafka starts. Wait 30 seconds after `docker-compose up` and check `docker-compose ps`.

**Elasticsearch container exits with code 78**
Your system's `vm.max_map_count` is too low. Fix with:
```bash
sudo sysctl -w vm.max_map_count=262144
```

**"Cannot transition from X to Y"**
The order state machine rejected your status update. Valid flow: `pending → confirmed → preparing → ready → picked_up → delivered`. Each step must happen in order.

**Port already in use**
Something on your machine is using port 3000, 9092, or 9200. Find it with `lsof -i :PORT` and stop it, or change the port mapping in `docker-compose.yml`.

---

## Contributing

1. Fork the repo
2. Create a feature branch: `git checkout -b feature/payment-service`
3. Commit with conventional commits: `git commit -m "feat(orders): add payment retry logic"`
4. Push and open a PR

Please write tests for new features and make sure `npm test` passes before opening a PR.

---

## License

MIT — build anything you want with this.