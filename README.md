# blackroad-service-mesh

> BlackRoad Cloud Infrastructure: blackroad-service-mesh

Part of the [BlackRoad OS](https://blackroad.io) ecosystem — [BlackRoad-Cloud](https://github.com/BlackRoad-Cloud)

---

# blackroad-service-mesh

> Production-quality service mesh with load balancing, traffic policies, circuit breaking, and topology export.

## Features

- **Service registration** with multiple endpoints and namespace isolation
- **Load balancing**: round_robin, least_conn, random
- **Traffic policies** per source→destination pair with weight, timeout, retries
- **Circuit breaker** — CLOSED / OPEN / HALF_OPEN state machine
- **Mesh topology** — graph of all services and edges
- **Config export** — full mesh snapshot as JSON
- **Health checking** via HTTP/HTTPS probe
- **SQLite persistence** — `~/.blackroad/service_mesh.db`

## Quick start

```bash
pip install -r requirements.txt
python src/service_mesh.py register api --endpoints 10.0.0.1:8080,10.0.0.2:8080 --protocol http --port 8080
python src/service_mesh.py list
python src/service_mesh.py route frontend api
python src/service_mesh.py topology
python src/service_mesh.py export
```

## API

```python
from src.service_mesh import register_service, route, apply_policy, mesh_topology, export_config

db = _get_db()

# Register
svc = register_service("api", ["10.0.0.1", "10.0.0.2"], "http", 8080, db=db)

# Route (round robin by default)
result = route("frontend", "api", db=db)
print(result.endpoint, result.latency_ms)

# Traffic policy
policy = apply_policy("frontend", "api",
    weight=100, timeout_ms=3000, retry_count=3,
    circuit_breaker_threshold=5, db=db)

# Check circuit breaker
cb = check_circuit_breaker(policy.id, db=db)
print(cb["circuit_state"])   # closed / open / half_open

# Record outcome
record_outcome("frontend", "api", success=False, db=db)

# Topology
topo = mesh_topology(db=db)
# {"nodes": [...], "edges": [...]}

# Export full config
cfg = export_config(db=db)
```

## Testing

```bash
pytest tests/ -v
```

## Architecture

```
Services ──register──▶ SQLite
    │
    └──route──▶ LoadBalancer (rr/lc/random)
                    │
                    └──▶ TrafficPolicy check
                              │
                              └──▶ CircuitBreaker state machine
```
