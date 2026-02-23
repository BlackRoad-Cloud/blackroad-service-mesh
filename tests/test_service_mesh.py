"""Tests for service_mesh.py"""
import json
import time
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from service_mesh import (
    register_service, deregister_service, get_service, list_services,
    apply_policy, list_policies, check_circuit_breaker, route,
    mesh_topology, export_config, health_check, record_outcome,
    _get_db, Protocol, LoadBalance, CircuitState,
)


@pytest.fixture
def db(tmp_path):
    return _get_db(tmp_path / "test_mesh.db")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def test_register_service(db):
    svc = register_service("api", ["10.0.0.1"], "http", 8080, db=db)
    assert svc.name == "api"
    assert svc.protocol == "http"
    assert svc.port == 8080


def test_register_service_defaults(db):
    svc = register_service("web", ["10.0.0.2"], "https", 443, db=db)
    assert svc.health_check_path == "/health"
    assert svc.load_balance == LoadBalance.ROUND_ROBIN.value


def test_register_service_upsert(db):
    svc1 = register_service("svc", ["ep1"], "http", 80, db=db)
    svc2 = register_service("svc", ["ep2"], "http", 80, db=db)
    assert svc1.name == svc2.name
    svcs = list_services(db=db)
    assert sum(1 for s in svcs if s.name == "svc") == 1


def test_deregister_service(db):
    register_service("gone", ["x"], "http", 80, db=db)
    ok = deregister_service("gone", db=db)
    assert ok
    assert get_service("gone", db=db) is None


def test_list_services_filtered(db):
    register_service("s1", ["e1"], "http", 80, namespace="prod", db=db)
    register_service("s2", ["e2"], "http", 80, namespace="dev", db=db)
    prod = list_services(namespace="prod", db=db)
    assert all(s.namespace == "prod" for s in prod)


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def test_route_round_robin(db):
    register_service("backend", ["ep1", "ep2", "ep3"], "http", 9000,
                     load_balance=LoadBalance.ROUND_ROBIN.value, db=db)
    endpoints = set()
    for _ in range(6):
        result = route("frontend", "backend", db=db)
        assert result.success
        endpoints.add(result.endpoint)
    assert len(endpoints) > 1  # used multiple endpoints


def test_route_random(db):
    register_service("rand-svc", ["a", "b", "c"], "http", 80,
                     load_balance=LoadBalance.RANDOM.value, db=db)
    for _ in range(5):
        r = route("caller", "rand-svc", db=db)
        assert r.success
        assert r.endpoint in ["a", "b", "c"]


def test_route_unknown_service(db):
    result = route("src", "nonexistent", db=db)
    assert not result.success
    assert "not found" in result.error


# ---------------------------------------------------------------------------
# Traffic policies
# ---------------------------------------------------------------------------

def test_apply_policy(db):
    p = apply_policy("frontend", "backend", weight=100, timeout_ms=3000, db=db)
    assert p.source == "frontend"
    assert p.destination == "backend"
    assert p.timeout_ms == 3000


def test_apply_policy_upsert(db):
    apply_policy("a", "b", timeout_ms=1000, db=db)
    p2 = apply_policy("a", "b", timeout_ms=2000, db=db)
    policies = list_policies(db=db)
    ab = [p for p in policies if p.source == "a" and p.destination == "b"]
    assert len(ab) == 1


def test_check_circuit_breaker(db):
    register_service("target", ["ep1"], "http", 80, db=db)
    p = apply_policy("caller", "target", circuit_breaker_threshold=3, db=db)
    cb = check_circuit_breaker(p.id, db=db)
    assert cb["circuit_state"] == CircuitState.CLOSED.value
    assert cb["failure_count"] == 0


def test_check_circuit_breaker_invalid(db):
    result = check_circuit_breaker("bad-id", db=db)
    assert "error" in result


# ---------------------------------------------------------------------------
# Circuit breaker state changes
# ---------------------------------------------------------------------------

def test_record_failures_opens_circuit(db):
    register_service("flaky", ["ep"], "http", 80, db=db)
    p = apply_policy("c", "flaky", circuit_breaker_threshold=3, db=db)
    for _ in range(3):
        record_outcome("c", "flaky", success=False, db=db)
    cb = check_circuit_breaker(p.id, db=db)
    assert cb["circuit_state"] == CircuitState.OPEN.value


def test_record_success_resets_circuit(db):
    register_service("ok-svc", ["ep"], "http", 80, db=db)
    p = apply_policy("c2", "ok-svc", circuit_breaker_threshold=3, db=db)
    for _ in range(3):
        record_outcome("c2", "ok-svc", success=False, db=db)
    record_outcome("c2", "ok-svc", success=True, db=db)
    cb = check_circuit_breaker(p.id, db=db)
    assert cb["circuit_state"] == CircuitState.CLOSED.value


# ---------------------------------------------------------------------------
# Topology & export
# ---------------------------------------------------------------------------

def test_mesh_topology_structure(db):
    register_service("t1", ["e1"], "http", 80, db=db)
    register_service("t2", ["e2"], "http", 80, db=db)
    apply_policy("t1", "t2", db=db)
    topo = mesh_topology(db=db)
    assert "nodes" in topo
    assert "edges" in topo
    assert topo["service_count"] >= 2
    assert topo["policy_count"] >= 1


def test_export_config_structure(db):
    register_service("ex1", ["e1"], "http", 80, db=db)
    cfg = export_config(db=db)
    assert "version" in cfg
    assert "topology" in cfg
    assert "exported_at" in cfg


# ---------------------------------------------------------------------------
# Service data model
# ---------------------------------------------------------------------------

def test_service_base_url():
    from service_mesh import Service
    svc = Service(name="x", namespace="default", endpoints=["10.0.0.1"],
                  protocol="https", port=8443)
    assert svc.base_url() == "https://10.0.0.1:8443"


def test_service_health_url():
    from service_mesh import Service
    svc = Service(name="x", namespace="default", endpoints=["10.0.0.1"],
                  protocol="http", port=80, health_check_path="/ping")
    assert svc.health_url() == "http://10.0.0.1:80/ping"
