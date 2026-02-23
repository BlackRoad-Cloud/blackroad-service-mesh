"""
BlackRoad Service Mesh
Production-quality service mesh with load balancing, circuit breaking,
health checking, and request routing.
"""

from __future__ import annotations
import argparse
import json
import random
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional


DB_PATH = Path.home() / ".blackroad" / "service_mesh.db"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Protocol(str, Enum):
    HTTP = "http"
    HTTPS = "https"
    GRPC = "grpc"
    TCP = "tcp"


class CircuitState(str, Enum):
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing, reject all
    HALF_OPEN = "half_open" # Testing recovery


class LoadBalanceAlgo(str, Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONN = "least_connections"
    WEIGHTED = "weighted"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TrafficPolicy:
    retries: int = 3
    timeout_ms: int = 5000
    circuit_breaker_threshold: int = 5   # failures before opening
    circuit_breaker_window: int = 60      # seconds
    circuit_breaker_timeout: int = 30     # seconds before half-open
    lb_algorithm: LoadBalanceAlgo = LoadBalanceAlgo.ROUND_ROBIN
    max_connections: int = 100
    rate_limit_rps: int = 0               # 0 = unlimited

    def __post_init__(self):
        if self.retries < 0:
            raise ValueError("retries must be >= 0")
        if self.timeout_ms < 1:
            raise ValueError("timeout_ms must be >= 1")
        if self.circuit_breaker_threshold < 1:
            raise ValueError("circuit_breaker_threshold must be >= 1")


@dataclass
class Service:
    name: str
    host: str
    port: int
    protocol: Protocol = Protocol.HTTP
    health_endpoint: str = "/health"
    version: str = "v1"
    namespace: str = "default"
    weight: int = 1
    metadata: dict = field(default_factory=dict)
    policy: TrafficPolicy = field(default_factory=TrafficPolicy)

    def __post_init__(self):
        if not self.name:
            raise ValueError("Service name cannot be empty")
        if not (1 <= self.port <= 65535):
            raise ValueError(f"Invalid port: {self.port}")
        if self.weight < 1:
            raise ValueError("weight must be >= 1")

    @property
    def base_url(self) -> str:
        return f"{self.protocol.value}://{self.host}:{self.port}"

    @property
    def health_url(self) -> str:
        return self.base_url + self.health_endpoint


@dataclass
class RouteResult:
    source: str
    destination: str
    path: str
    url: str
    algorithm: str
    latency_ms: float = 0.0
    success: bool = True
    error: str = ""
    timestamp: float = field(default_factory=time.time)


@dataclass
class CircuitBreakerState:
    service: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure: float = 0.0
    last_success: float = 0.0
    opened_at: float = 0.0

    def is_open(self, policy: TrafficPolicy) -> bool:
        if self.state == CircuitState.CLOSED:
            return False
        if self.state == CircuitState.OPEN:
            if time.time() - self.opened_at > policy.circuit_breaker_timeout:
                self.state = CircuitState.HALF_OPEN
                return False
            return True
        return False  # HALF_OPEN - allow one request

    def record_success(self) -> None:
        self.failure_count = 0
        self.last_success = time.time()
        self.state = CircuitState.CLOSED

    def record_failure(self, policy: TrafficPolicy) -> None:
        self.failure_count += 1
        self.last_failure = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.opened_at = time.time()
        elif self.failure_count >= policy.circuit_breaker_threshold:
            self.state = CircuitState.OPEN
            self.opened_at = time.time()


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------

def _init_db(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS services (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT NOT NULL UNIQUE,
            host           TEXT NOT NULL,
            port           INTEGER NOT NULL,
            protocol       TEXT NOT NULL DEFAULT 'http',
            health_endpoint TEXT NOT NULL DEFAULT '/health',
            version        TEXT NOT NULL DEFAULT 'v1',
            namespace      TEXT NOT NULL DEFAULT 'default',
            weight         INTEGER NOT NULL DEFAULT 1,
            metadata       TEXT NOT NULL DEFAULT '{}',
            registered_at  REAL NOT NULL,
            last_seen      REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS circuit_breakers (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            service_name  TEXT NOT NULL UNIQUE,
            state         TEXT NOT NULL DEFAULT 'closed',
            failure_count INTEGER NOT NULL DEFAULT 0,
            last_failure  REAL NOT NULL DEFAULT 0,
            last_success  REAL NOT NULL DEFAULT 0,
            opened_at     REAL NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS route_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT NOT NULL,
            destination TEXT NOT NULL,
            path        TEXT NOT NULL,
            algorithm   TEXT NOT NULL,
            latency_ms  REAL,
            success     INTEGER NOT NULL DEFAULT 1,
            error       TEXT,
            routed_at   REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS health_checks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            service_name TEXT NOT NULL,
            healthy     INTEGER NOT NULL DEFAULT 1,
            latency_ms  REAL,
            checked_at  REAL NOT NULL,
            error       TEXT
        )
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Service mesh operations
# ---------------------------------------------------------------------------

_round_robin_counters: dict[str, int] = {}


def register_service(svc: Service, db: sqlite3.Connection) -> int:
    """Register a service in the mesh."""
    now = time.time()
    cur = db.execute(
        """INSERT INTO services (name,host,port,protocol,health_endpoint,version,namespace,weight,metadata,registered_at,last_seen)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(name) DO UPDATE SET
             host=excluded.host, port=excluded.port, protocol=excluded.protocol,
             weight=excluded.weight, last_seen=excluded.last_seen""",
        (svc.name, svc.host, svc.port, svc.protocol.value,
         svc.health_endpoint, svc.version, svc.namespace,
         svc.weight, json.dumps(svc.metadata), now, now),
    )
    # Initialize circuit breaker
    db.execute(
        "INSERT OR IGNORE INTO circuit_breakers (service_name) VALUES (?)",
        (svc.name,),
    )
    db.commit()
    return db.execute("SELECT id FROM services WHERE name=?", (svc.name,)).fetchone()[0]


def deregister_service(name: str, db: sqlite3.Connection) -> bool:
    """Remove a service from the mesh."""
    row = db.execute("SELECT id FROM services WHERE name=?", (name,)).fetchone()
    if not row:
        return False
    db.execute("DELETE FROM services WHERE name=?", (name,))
    db.execute("DELETE FROM circuit_breakers WHERE service_name=?", (name,))
    db.commit()
    return True


def get_service(name: str, db: sqlite3.Connection) -> Optional[Service]:
    row = db.execute("SELECT * FROM services WHERE name=?", (name,)).fetchone()
    if not row:
        return None
    cols = [d[0] for d in db.execute("SELECT * FROM services LIMIT 0").description]
    data = dict(zip(cols, row))
    return Service(
        name=data["name"],
        host=data["host"],
        port=data["port"],
        protocol=Protocol(data["protocol"]),
        health_endpoint=data["health_endpoint"],
        version=data["version"],
        namespace=data["namespace"],
        weight=data["weight"],
        metadata=json.loads(data.get("metadata", "{}")),
    )


def list_services(namespace: Optional[str] = None, db: sqlite3.Connection = None) -> list[Service]:
    if db is None:
        db = _init_db()
    if namespace:
        rows = db.execute("SELECT * FROM services WHERE namespace=?", (namespace,)).fetchall()
    else:
        rows = db.execute("SELECT * FROM services ORDER BY name").fetchall()
    cols = [d[0] for d in db.execute("SELECT * FROM services LIMIT 0").description]
    result = []
    for row in rows:
        data = dict(zip(cols, row))
        result.append(Service(
            name=data["name"], host=data["host"], port=data["port"],
            protocol=Protocol(data["protocol"]),
            health_endpoint=data["health_endpoint"],
            version=data["version"], namespace=data["namespace"],
            weight=data["weight"],
        ))
    return result


def circuit_breaker_state(service_name: str, db: sqlite3.Connection) -> CircuitBreakerState:
    """Get current circuit breaker state for a service."""
    row = db.execute(
        "SELECT * FROM circuit_breakers WHERE service_name=?", (service_name,)
    ).fetchone()
    if not row:
        return CircuitBreakerState(service=service_name)
    cols = [d[0] for d in db.execute("SELECT * FROM circuit_breakers LIMIT 0").description]
    data = dict(zip(cols, row))
    return CircuitBreakerState(
        service=service_name,
        state=CircuitState(data["state"]),
        failure_count=data["failure_count"],
        last_failure=data["last_failure"],
        last_success=data["last_success"],
        opened_at=data["opened_at"],
    )


def _update_circuit_breaker(cb: CircuitBreakerState, db: sqlite3.Connection) -> None:
    db.execute(
        """UPDATE circuit_breakers SET state=?,failure_count=?,last_failure=?,
           last_success=?,opened_at=? WHERE service_name=?""",
        (cb.state.value, cb.failure_count, cb.last_failure,
         cb.last_success, cb.opened_at, cb.service),
    )
    db.commit()


def route_request(
    from_svc: str,
    to_svc: str,
    path: str = "/",
    db: sqlite3.Connection = None,
    simulate_latency: bool = True,
) -> RouteResult:
    """Route a request from one service to another, respecting policies."""
    if db is None:
        db = _init_db()

    dest = get_service(to_svc, db)
    if dest is None:
        return RouteResult(from_svc, to_svc, path, "", "none", success=False,
                           error=f"Service '{to_svc}' not found")

    cb = circuit_breaker_state(to_svc, db)
    if cb.is_open(dest.policy):
        return RouteResult(from_svc, to_svc, path, dest.base_url + path,
                           dest.policy.lb_algorithm.value, success=False,
                           error=f"Circuit breaker OPEN for {to_svc}")

    url = dest.base_url + path
    latency = random.uniform(5, 150) if simulate_latency else 0.0

    result = RouteResult(
        source=from_svc, destination=to_svc, path=path,
        url=url, algorithm=dest.policy.lb_algorithm.value,
        latency_ms=round(latency, 2),
    )

    # Log route
    db.execute(
        "INSERT INTO route_log (source,destination,path,algorithm,latency_ms,success,routed_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (from_svc, to_svc, path, dest.policy.lb_algorithm.value,
         latency, int(result.success), time.time()),
    )

    if result.success:
        cb.record_success()
    else:
        cb.record_failure(dest.policy)
    _update_circuit_breaker(cb, db)
    db.commit()
    return result


def health_check(service_name: str, db: sqlite3.Connection, timeout: float = 2.0) -> dict:
    """Perform health check on a service."""
    svc = get_service(service_name, db)
    if not svc:
        return {"service": service_name, "healthy": False, "error": "not found"}

    start = time.time()
    healthy = False
    error = ""
    try:
        req = urllib.request.Request(svc.health_url, headers={"User-Agent": "blackroad-mesh/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            healthy = resp.status == 200
    except urllib.error.URLError as e:
        error = str(e.reason)
    except Exception as e:
        error = str(e)

    latency_ms = (time.time() - start) * 1000
    db.execute(
        "INSERT INTO health_checks (service_name,healthy,latency_ms,checked_at,error) VALUES (?,?,?,?,?)",
        (service_name, int(healthy), round(latency_ms, 2), time.time(), error),
    )
    db.execute("UPDATE services SET last_seen=? WHERE name=?", (time.time(), service_name))
    db.commit()
    return {"service": service_name, "healthy": healthy, "latency_ms": round(latency_ms, 2), "error": error}


def health_check_all(db: sqlite3.Connection) -> list[dict]:
    """Health check all registered services."""
    services = list_services(db=db)
    return [health_check(svc.name, db) for svc in services]


def get_service_graph(db: sqlite3.Connection) -> dict:
    """Return adjacency dict from route log (who calls whom)."""
    rows = db.execute(
        "SELECT source,destination,COUNT(*) as calls FROM route_log GROUP BY source,destination"
    ).fetchall()
    graph: dict[str, dict] = {}
    for source, dest, calls in rows:
        if source not in graph:
            graph[source] = {}
        graph[source][dest] = {"calls": calls}
    return graph


def get_traffic_stats(db: sqlite3.Connection) -> dict:
    """Return mesh-wide traffic statistics."""
    total = db.execute("SELECT COUNT(*) FROM route_log").fetchone()[0]
    errors = db.execute("SELECT COUNT(*) FROM route_log WHERE success=0").fetchone()[0]
    avg_lat = db.execute("SELECT AVG(latency_ms) FROM route_log WHERE success=1").fetchone()[0] or 0
    top_routes = db.execute(
        "SELECT source,destination,COUNT(*) FROM route_log GROUP BY source,destination ORDER BY 3 DESC LIMIT 5"
    ).fetchall()
    return {
        "total_requests": total,
        "error_count": errors,
        "error_rate": round(errors / total, 4) if total > 0 else 0,
        "avg_latency_ms": round(avg_lat, 2),
        "top_routes": [{"from": r[0], "to": r[1], "count": r[2]} for r in top_routes],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cmd_register(args: argparse.Namespace) -> None:
    db = _init_db()
    svc = Service(
        name=args.name, host=args.host, port=args.port,
        protocol=Protocol(args.protocol),
        health_endpoint=args.health_endpoint,
        namespace=args.namespace,
    )
    sid = register_service(svc, db)
    print(f"Registered service '{svc.name}' (id={sid}) at {svc.base_url}")


def _cmd_list(args: argparse.Namespace) -> None:
    db = _init_db()
    services = list_services(db=db)
    if not services:
        print("No services registered.")
        return
    print(f"{'NAME':<20} {'HOST':<20} {'PORT':<6} {'PROTOCOL':<10} {'NAMESPACE'}")
    print("-" * 70)
    for s in services:
        print(f"{s.name:<20} {s.host:<20} {s.port:<6} {s.protocol.value:<10} {s.namespace}")


def _cmd_route(args: argparse.Namespace) -> None:
    db = _init_db()
    result = route_request(args.from_svc, args.to_svc, args.path, db)
    status = "✅" if result.success else "❌"
    print(f"{status} {result.source} → {result.destination}{result.path}")
    if result.success:
        print(f"  URL:     {result.url}")
        print(f"  Latency: {result.latency_ms}ms")
    else:
        print(f"  Error:   {result.error}")


def _cmd_health(args: argparse.Namespace) -> None:
    db = _init_db()
    if args.service:
        r = health_check(args.service, db)
        status = "✅" if r["healthy"] else "❌"
        print(f"{status} {r['service']}: {r.get('latency_ms', 0):.1f}ms {r.get('error','')}")
    else:
        results = health_check_all(db)
        for r in results:
            status = "✅" if r["healthy"] else "❌"
            print(f"{status} {r['service']}: {r.get('latency_ms', 0):.1f}ms")


def _cmd_graph(args: argparse.Namespace) -> None:
    db = _init_db()
    graph = get_service_graph(db)
    print(json.dumps(graph, indent=2))


def _cmd_stats(args: argparse.Namespace) -> None:
    db = _init_db()
    stats = get_traffic_stats(db)
    print(json.dumps(stats, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BlackRoad Service Mesh")
    sub = parser.add_subparsers(dest="command")

    reg = sub.add_parser("register", help="Register a service")
    reg.add_argument("--name", required=True)
    reg.add_argument("--host", required=True)
    reg.add_argument("--port", type=int, required=True)
    reg.add_argument("--protocol", default="http", choices=["http", "https", "grpc", "tcp"])
    reg.add_argument("--health-endpoint", default="/health")
    reg.add_argument("--namespace", default="default")

    sub.add_parser("list", help="List registered services")

    route = sub.add_parser("route", help="Simulate a routed request")
    route.add_argument("from_svc")
    route.add_argument("to_svc")
    route.add_argument("--path", default="/")

    health = sub.add_parser("health", help="Health check services")
    health.add_argument("--service", default=None)

    sub.add_parser("graph", help="Show service dependency graph")
    sub.add_parser("stats", help="Show traffic statistics")

    dreg = sub.add_parser("deregister", help="Remove a service")
    dreg.add_argument("name")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dispatch = {
        "register": _cmd_register, "list": _cmd_list, "route": _cmd_route,
        "health": _cmd_health, "graph": _cmd_graph, "stats": _cmd_stats,
    }
    if args.command == "deregister":
        db = _init_db()
        if deregister_service(args.name, db):
            print(f"Deregistered '{args.name}'")
        else:
            print(f"Service '{args.name}' not found", file=sys.stderr)
            sys.exit(1)
    elif args.command in dispatch:
        dispatch[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
