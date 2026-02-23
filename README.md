# BlackRoad Service Mesh

> Service discovery, intelligent traffic routing, circuit breaking, and health monitoring.

## Features

- `Service` dataclass (name, host, port, protocol, health_endpoint, weight)
- `TrafficPolicy` (retries, timeout_ms, circuit_breaker_threshold, lb_algorithm)
- `register_service()`, `deregister_service()` — service registry
- `route_request()` — request routing with load balancing and circuit breaker enforcement
- `circuit_breaker_state()` — CLOSED → OPEN → HALF_OPEN state machine
- `health_check_all()` — HTTP health checks across all services
- `get_service_graph()` — adjacency dict from route log
- `get_traffic_stats()` — error rate, avg latency, top routes
- SQLite persistence (services, circuit breakers, route log, health log)
- CLI: `register`, `list`, `route`, `health`, `graph`, `stats`, `deregister`

## Usage

```bash
# Register services
python src/service_mesh.py register --name api-gateway --host gw.internal --port 8080
python src/service_mesh.py register --name user-service --host users.internal --port 3001
python src/service_mesh.py register --name order-service --host orders.internal --port 3002 --namespace prod

# Route a request
python src/service_mesh.py route api-gateway user-service --path /users/me

# Health check all
python src/service_mesh.py health

# View service graph
python src/service_mesh.py graph

# Traffic stats
python src/service_mesh.py stats
```

## Circuit Breaker States

```
CLOSED ──(failures >= threshold)──► OPEN ──(timeout expired)──► HALF_OPEN
  ▲                                                                  │
  └──────────────────(success)──────────────────────────────────────┘
```

## Tests

```bash
pytest tests/ -v --cov=src
```
