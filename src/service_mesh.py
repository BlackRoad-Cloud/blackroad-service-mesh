"""
BlackRoad Service Mesh
Production-quality service mesh with load balancing, traffic policies,
circuit breaking, health checking, topology export, and config export.
"""

from __future__ import annotations

import json
import random
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional
import urllib.request
import urllib.error

DB_PATH = Path.home() / ".blackroad" / "service_mesh.db"

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Protocol(str, Enum):
    HTTP  = "http"
    HTTPS = "https"
    GRPC  = "grpc"
    TCP   = "tcp"


class LoadBalance(str, Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_CONN  = "least_conn"
    RANDOM      = "random"
    WEIGHTED    = "weighted"


class CircuitState(str, Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Service:
    name: str
    namespace: str
    endpoints: list[str]
    protocol: str
    port: int
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    health_check_path: str = "/health"
    load_balance: str = LoadBalance.ROUND_ROBIN.value
    weight: int = 1
    created_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def base_url(self, endpoint: Optional[str] = None) -> str:
        ep = endpoint or (self.endpoints[0] if self.endpoints else "localhost")
        return f"{self.protocol}://{ep}:{self.port}"

    def health_url(self, endpoint: Optional[str] = None) -> str:
        return f"{self.base_url(endpoint)}{self.health_check_path}"


@dataclass
class TrafficPolicy:
    source: str
    destination: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    weight: int = 100
    timeout_ms: int = 5000
    retry_count: int = 3
    circuit_breaker_threshold: int = 5   # failures before opening circuit
    circuit_breaker_sleep_ms: int = 30000
    headers: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass
class RouteResult:
    service_name: str
    endpoint: str
    policy_id: Optional[str]
    latency_ms: float = 0.0
    success: bool = True
    error: str = ""


@dataclass
class CircuitBreakerState:
    service_name: str
    state: str = CircuitState.CLOSED.value
    failure_count: int = 0
    last_failure_ts: float = 0.0
    last_success_ts: float = 0.0

    def is_open(self, policy: TrafficPolicy) -> bool:
        if self.state == CircuitState.OPEN.value:
            elapsed = (time.time() - self.last_failure_ts) * 1000
            if elapsed >= policy.circuit_breaker_sleep_ms:
                self.state = CircuitState.HALF_OPEN.value
                return False
            return True
        return False

    def record_success(self) -> None:
        self.failure_count = 0
        self.last_success_ts = time.time()
        self.state = CircuitState.CLOSED.value

    def record_failure(self, policy: TrafficPolicy) -> None:
        self.failure_count += 1
        self.last_failure_ts = time.time()
        if self.failure_count >= policy.circuit_breaker_threshold:
            self.state = CircuitState.OPEN.value


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _get_db(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS services (
            id               TEXT PRIMARY KEY,
            name             TEXT NOT NULL,
            namespace        TEXT NOT NULL DEFAULT 'default',
            endpoints        TEXT NOT NULL DEFAULT '[]',
            protocol         TEXT NOT NULL DEFAULT 'http',
            port             INTEGER NOT NULL DEFAULT 80,
            health_check_path TEXT NOT NULL DEFAULT '/health',
            load_balance     TEXT NOT NULL DEFAULT 'round_robin',
            weight           INTEGER NOT NULL DEFAULT 1,
            metadata         TEXT NOT NULL DEFAULT '{}',
            created_at       REAL NOT NULL,
            UNIQUE(name, namespace)
        );

        CREATE TABLE IF NOT EXISTS traffic_policies (
            id                          TEXT PRIMARY KEY,
            source                      TEXT NOT NULL,
            destination                 TEXT NOT NULL,
            weight                      INTEGER NOT NULL DEFAULT 100,
            timeout_ms                  INTEGER NOT NULL DEFAULT 5000,
            retry_count                 INTEGER NOT NULL DEFAULT 3,
            circuit_breaker_threshold   INTEGER NOT NULL DEFAULT 5,
            circuit_breaker_sleep_ms    INTEGER NOT NULL DEFAULT 30000,
            headers                     TEXT NOT NULL DEFAULT '{}',
            created_at                  REAL NOT NULL,
            UNIQUE(source, destination)
        );

        CREATE TABLE IF NOT EXISTS circuit_breakers (
            service_name    TEXT PRIMARY KEY,
            state           TEXT NOT NULL DEFAULT 'closed',
            failure_count   INTEGER NOT NULL DEFAULT 0,
            last_failure_ts REAL NOT NULL DEFAULT 0,
            last_success_ts REAL NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS rr_counters (
            service_name TEXT PRIMARY KEY,
            counter      INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS conn_counts (
            service_name    TEXT NOT NULL,
            endpoint        TEXT NOT NULL,
            active_conns    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(service_name, endpoint)
        );

        CREATE TABLE IF NOT EXISTS request_log (
            id          TEXT PRIMARY KEY,
            source      TEXT NOT NULL,
            destination TEXT NOT NULL,
            endpoint    TEXT NOT NULL,
            success     INTEGER NOT NULL,
            latency_ms  REAL NOT NULL,
            timestamp   REAL NOT NULL
        );
    """)
    conn.commit()


def _row_to_service(row: sqlite3.Row) -> Service:
    return Service(
        id=row["id"],
        name=row["name"],
        namespace=row["namespace"],
        endpoints=json.loads(row["endpoints"]),
        protocol=row["protocol"],
        port=row["port"],
        health_check_path=row["health_check_path"],
        load_balance=row["load_balance"],
        weight=row["weight"],
        created_at=row["created_at"],
        metadata=json.loads(row["metadata"]),
    )


def _row_to_policy(row: sqlite3.Row) -> TrafficPolicy:
    return TrafficPolicy(
        id=row["id"],
        source=row["source"],
        destination=row["destination"],
        weight=row["weight"],
        timeout_ms=row["timeout_ms"],
        retry_count=row["retry_count"],
        circuit_breaker_threshold=row["circuit_breaker_threshold"],
        circuit_breaker_sleep_ms=row["circuit_breaker_sleep_ms"],
        headers=json.loads(row["headers"]),
        created_at=row["created_at"],
    )


# ---------------------------------------------------------------------------
# Service registration
# ---------------------------------------------------------------------------

def register_service(
    name: str,
    endpoints: list[str],
    protocol: str,
    port: int,
    namespace: str = "default",
    health_check_path: str = "/health",
    load_balance: str = LoadBalance.ROUND_ROBIN.value,
    weight: int = 1,
    metadata: Optional[dict] = None,
    db: Optional[sqlite3.Connection] = None,
) -> Service:
    """Register a new service in the mesh."""
    conn = db or _get_db()
    svc = Service(
        name=name,
        namespace=namespace,
        endpoints=endpoints,
        protocol=protocol,
        port=port,
        health_check_path=health_check_path,
        load_balance=load_balance,
        weight=weight,
        metadata=metadata or {},
    )
    conn.execute(
        """INSERT OR REPLACE INTO services
           (id,name,namespace,endpoints,protocol,port,health_check_path,
            load_balance,weight,metadata,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (svc.id, svc.name, svc.namespace, json.dumps(svc.endpoints),
         svc.protocol, svc.port, svc.health_check_path, svc.load_balance,
         svc.weight, json.dumps(svc.metadata), svc.created_at),
    )
    conn.commit()
    return svc


def deregister_service(name: str, namespace: str = "default", db: Optional[sqlite3.Connection] = None) -> bool:
    conn = db or _get_db()
    cur = conn.execute("DELETE FROM services WHERE name=? AND namespace=?", (name, namespace))
    conn.commit()
    return cur.rowcount > 0


def get_service(name: str, namespace: str = "default", db: Optional[sqlite3.Connection] = None) -> Optional[Service]:
    conn = db or _get_db()
    row = conn.execute("SELECT * FROM services WHERE name=? AND namespace=?", (name, namespace)).fetchone()
    return _row_to_service(row) if row else None


def list_services(namespace: Optional[str] = None, db: Optional[sqlite3.Connection] = None) -> list[Service]:
    conn = db or _get_db()
    if namespace:
        rows = conn.execute("SELECT * FROM services WHERE namespace=?", (namespace,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM services").fetchall()
    return [_row_to_service(r) for r in rows]


# ---------------------------------------------------------------------------
# Load balancing
# ---------------------------------------------------------------------------

def _pick_endpoint_round_robin(svc: Service, db: sqlite3.Connection) -> str:
    if not svc.endpoints:
        raise RuntimeError(f"No endpoints for {svc.name}")
    row = db.execute("SELECT counter FROM rr_counters WHERE service_name=?", (svc.name,)).fetchone()
    idx = row["counter"] if row else 0
    endpoint = svc.endpoints[idx % len(svc.endpoints)]
    db.execute(
        "INSERT OR REPLACE INTO rr_counters (service_name, counter) VALUES (?,?)",
        (svc.name, idx + 1),
    )
    db.commit()
    return endpoint


def _pick_endpoint_least_conn(svc: Service, db: sqlite3.Connection) -> str:
    if not svc.endpoints:
        raise RuntimeError(f"No endpoints for {svc.name}")
    counts: dict[str, int] = {}
    for ep in svc.endpoints:
        row = db.execute(
            "SELECT active_conns FROM conn_counts WHERE service_name=? AND endpoint=?",
            (svc.name, ep),
        ).fetchone()
        counts[ep] = row["active_conns"] if row else 0
    return min(counts, key=lambda e: counts[e])


def _pick_endpoint_random(svc: Service, _db: sqlite3.Connection) -> str:
    if not svc.endpoints:
        raise RuntimeError(f"No endpoints for {svc.name}")
    return random.choice(svc.endpoints)


def _pick_endpoint(svc: Service, db: sqlite3.Connection) -> str:
    algo = svc.load_balance
    if algo == LoadBalance.ROUND_ROBIN.value:
        return _pick_endpoint_round_robin(svc, db)
    if algo == LoadBalance.LEAST_CONN.value:
        return _pick_endpoint_least_conn(svc, db)
    return _pick_endpoint_random(svc, db)


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def route(
    source: str,
    dest_service: str,
    namespace: str = "default",
    db: Optional[sqlite3.Connection] = None,
) -> RouteResult:
    """Route a request from source to dest_service, applying traffic policies."""
    conn = db or _get_db()
    svc = get_service(dest_service, namespace, conn)
    if not svc:
        return RouteResult(dest_service, "", None, success=False, error=f"service {dest_service} not found")

    policy = _get_policy(source, dest_service, conn)
    cb = _load_circuit_breaker(dest_service, conn)

    if policy and cb.is_open(policy):
        return RouteResult(
            dest_service, "", policy.id, success=False,
            error=f"circuit breaker OPEN for {dest_service}",
        )

    start = time.time()
    endpoint = _pick_endpoint(svc, conn)
    latency = (time.time() - start) * 1000

    # Log request
    conn.execute(
        "INSERT INTO request_log (id,source,destination,endpoint,success,latency_ms,timestamp) VALUES (?,?,?,?,?,?,?)",
        (str(uuid.uuid4()), source, dest_service, endpoint, 1, latency, time.time()),
    )
    conn.commit()

    return RouteResult(
        service_name=dest_service,
        endpoint=endpoint,
        policy_id=policy.id if policy else None,
        latency_ms=latency,
        success=True,
    )


# ---------------------------------------------------------------------------
# Traffic policies
# ---------------------------------------------------------------------------

def apply_policy(
    source: str,
    destination: str,
    weight: int = 100,
    timeout_ms: int = 5000,
    retry_count: int = 3,
    circuit_breaker_threshold: int = 5,
    db: Optional[sqlite3.Connection] = None,
) -> TrafficPolicy:
    """Create or replace a traffic policy between source and destination."""
    conn = db or _get_db()
    p = TrafficPolicy(
        source=source,
        destination=destination,
        weight=weight,
        timeout_ms=timeout_ms,
        retry_count=retry_count,
        circuit_breaker_threshold=circuit_breaker_threshold,
    )
    conn.execute(
        """INSERT OR REPLACE INTO traffic_policies
           (id,source,destination,weight,timeout_ms,retry_count,
            circuit_breaker_threshold,circuit_breaker_sleep_ms,headers,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (p.id, p.source, p.destination, p.weight, p.timeout_ms, p.retry_count,
         p.circuit_breaker_threshold, p.circuit_breaker_sleep_ms, json.dumps(p.headers), p.created_at),
    )
    conn.commit()
    return p


def _get_policy(source: str, destination: str, db: sqlite3.Connection) -> Optional[TrafficPolicy]:
    row = db.execute(
        "SELECT * FROM traffic_policies WHERE source=? AND destination=?", (source, destination)
    ).fetchone()
    return _row_to_policy(row) if row else None


def list_policies(db: Optional[sqlite3.Connection] = None) -> list[TrafficPolicy]:
    conn = db or _get_db()
    return [_row_to_policy(r) for r in conn.execute("SELECT * FROM traffic_policies").fetchall()]


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

def _load_circuit_breaker(service_name: str, db: sqlite3.Connection) -> CircuitBreakerState:
    row = db.execute("SELECT * FROM circuit_breakers WHERE service_name=?", (service_name,)).fetchone()
    if row:
        return CircuitBreakerState(
            service_name=row["service_name"],
            state=row["state"],
            failure_count=row["failure_count"],
            last_failure_ts=row["last_failure_ts"],
            last_success_ts=row["last_success_ts"],
        )
    return CircuitBreakerState(service_name=service_name)


def _save_circuit_breaker(cb: CircuitBreakerState, db: sqlite3.Connection) -> None:
    db.execute(
        """INSERT OR REPLACE INTO circuit_breakers
           (service_name,state,failure_count,last_failure_ts,last_success_ts)
           VALUES (?,?,?,?,?)""",
        (cb.service_name, cb.state, cb.failure_count, cb.last_failure_ts, cb.last_success_ts),
    )
    db.commit()


def check_circuit_breaker(policy_id: str, db: Optional[sqlite3.Connection] = None) -> dict:
    """Return circuit breaker status for the policy's destination service."""
    conn = db or _get_db()
    row = conn.execute("SELECT * FROM traffic_policies WHERE id=?", (policy_id,)).fetchone()
    if not row:
        return {"error": f"policy {policy_id} not found"}
    policy = _row_to_policy(row)
    cb = _load_circuit_breaker(policy.destination, conn)
    return {
        "policy_id": policy_id,
        "source": policy.source,
        "destination": policy.destination,
        "circuit_state": cb.state,
        "failure_count": cb.failure_count,
        "threshold": policy.circuit_breaker_threshold,
        "last_failure": cb.last_failure_ts,
        "last_success": cb.last_success_ts,
    }


def record_outcome(
    source: str,
    destination: str,
    success: bool,
    db: Optional[sqlite3.Connection] = None,
) -> None:
    """Record a request outcome to update circuit breaker state."""
    conn = db or _get_db()
    policy = _get_policy(source, destination, conn)
    cb = _load_circuit_breaker(destination, conn)
    if success:
        cb.record_success()
    elif policy:
        cb.record_failure(policy)
    _save_circuit_breaker(cb, conn)


# ---------------------------------------------------------------------------
# Topology & config export
# ---------------------------------------------------------------------------

def mesh_topology(db: Optional[sqlite3.Connection] = None) -> dict:
    """Return a graph of all services and their traffic policies."""
    conn = db or _get_db()
    services = list_services(db=conn)
    policies = list_policies(db=conn)
    rows = conn.execute(
        "SELECT destination, COUNT(*) as reqs, AVG(latency_ms) as avg_lat, SUM(success) as ok FROM request_log GROUP BY destination"
    ).fetchall()
    stats: dict[str, dict] = {r["destination"]: dict(r) for r in rows}

    nodes = [
        {
            "id": s.id,
            "name": s.name,
            "namespace": s.namespace,
            "protocol": s.protocol,
            "port": s.port,
            "load_balance": s.load_balance,
            "endpoints": s.endpoints,
            "stats": stats.get(s.name, {}),
        }
        for s in services
    ]
    edges = [
        {
            "id": p.id,
            "source": p.source,
            "destination": p.destination,
            "weight": p.weight,
            "timeout_ms": p.timeout_ms,
            "retry_count": p.retry_count,
            "circuit_breaker_threshold": p.circuit_breaker_threshold,
        }
        for p in policies
    ]
    return {"nodes": nodes, "edges": edges, "service_count": len(nodes), "policy_count": len(edges)}


def export_config(db: Optional[sqlite3.Connection] = None) -> dict:
    """Export the full mesh configuration as a serializable dict."""
    conn = db or _get_db()
    topology = mesh_topology(db=conn)
    cb_rows = conn.execute("SELECT * FROM circuit_breakers").fetchall()
    circuit_breakers = [dict(r) for r in cb_rows]

    return {
        "version": "1.0",
        "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "topology": topology,
        "circuit_breakers": circuit_breakers,
    }


# ---------------------------------------------------------------------------
# Health checking
# ---------------------------------------------------------------------------

def health_check(service_name: str, namespace: str = "default", timeout: float = 2.0, db: Optional[sqlite3.Connection] = None) -> dict:
    conn = db or _get_db()
    svc = get_service(service_name, namespace, conn)
    if not svc:
        return {"service": service_name, "healthy": False, "error": "not found"}
    results = []
    for ep in svc.endpoints:
        url = f"{svc.protocol}://{ep}:{svc.port}{svc.health_check_path}"
        t0 = time.time()
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                latency = (time.time() - t0) * 1000
                results.append({"endpoint": ep, "status": resp.status, "latency_ms": latency, "healthy": resp.status < 400})
        except Exception as exc:
            results.append({"endpoint": ep, "healthy": False, "error": str(exc)})
    return {
        "service": service_name,
        "healthy": all(r["healthy"] for r in results),
        "endpoints": results,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli_main() -> None:
    import argparse, sys

    p = argparse.ArgumentParser(prog="service_mesh", description="BlackRoad Service Mesh")
    sub = p.add_subparsers(dest="cmd")

    reg = sub.add_parser("register", help="Register a service")
    reg.add_argument("name")
    reg.add_argument("--endpoints", required=True, help="comma-separated host:port list")
    reg.add_argument("--protocol", default="http")
    reg.add_argument("--port", type=int, default=80)
    reg.add_argument("--namespace", default="default")
    reg.add_argument("--lb", default="round_robin", dest="load_balance")

    sub.add_parser("list", help="List services")

    rt = sub.add_parser("route", help="Route from source to destination")
    rt.add_argument("source")
    rt.add_argument("dest")
    rt.add_argument("--namespace", default="default")

    pol = sub.add_parser("policy", help="Apply a traffic policy")
    pol.add_argument("source")
    pol.add_argument("dest")
    pol.add_argument("--weight", type=int, default=100)
    pol.add_argument("--timeout", type=int, default=5000, dest="timeout_ms")
    pol.add_argument("--retries", type=int, default=3, dest="retry_count")

    sub.add_parser("topology", help="Show mesh topology")
    sub.add_parser("export", help="Export mesh config as JSON")

    args = p.parse_args()
    db = _get_db()

    if args.cmd == "register":
        endpoints = [e.strip() for e in args.endpoints.split(",")]
        svc = register_service(args.name, endpoints, args.protocol, args.port,
                               namespace=args.namespace, load_balance=args.load_balance, db=db)
        print(json.dumps({"id": svc.id, "name": svc.name}, indent=2))
    elif args.cmd == "list":
        svcs = list_services(db=db)
        print(json.dumps([{"name": s.name, "ns": s.namespace, "endpoints": s.endpoints} for s in svcs], indent=2))
    elif args.cmd == "route":
        result = route(args.source, args.dest, namespace=args.namespace, db=db)
        print(json.dumps(result.__dict__, indent=2))
    elif args.cmd == "policy":
        pol_obj = apply_policy(args.source, args.dest, weight=args.weight,
                               timeout_ms=args.timeout_ms, retry_count=args.retry_count, db=db)
        print(json.dumps({"id": pol_obj.id}, indent=2))
    elif args.cmd == "topology":
        print(json.dumps(mesh_topology(db=db), indent=2))
    elif args.cmd == "export":
        print(json.dumps(export_config(db=db), indent=2))
    else:
        p.print_help()
        sys.exit(1)


if __name__ == "__main__":
    _cli_main()
