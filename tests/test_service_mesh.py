"""Tests for BlackRoad Service Mesh."""
import pytest
from service_mesh import (
    Service, TrafficPolicy, CircuitBreakerState, CircuitState, Protocol,
    register_service, deregister_service, get_service, list_services,
    circuit_breaker_state, route_request, health_check_all, get_service_graph,
    get_traffic_stats, _init_db,
)


def make_db(tmp_path):
    return _init_db(tmp_path / "test_mesh.db")


def make_svc(**kwargs):
    defaults = dict(name="api", host="localhost", port=8080)
    defaults.update(kwargs)
    return Service(**defaults)


class TestTrafficPolicy:
    def test_defaults(self):
        p = TrafficPolicy()
        assert p.retries == 3
        assert p.timeout_ms == 5000

    def test_invalid_retries(self):
        with pytest.raises(ValueError):
            TrafficPolicy(retries=-1)

    def test_invalid_timeout(self):
        with pytest.raises(ValueError):
            TrafficPolicy(timeout_ms=0)

    def test_invalid_threshold(self):
        with pytest.raises(ValueError):
            TrafficPolicy(circuit_breaker_threshold=0)


class TestService:
    def test_basic_service(self):
        svc = make_svc()
        assert svc.name == "api"
        assert svc.port == 8080

    def test_invalid_port(self):
        with pytest.raises(ValueError):
            make_svc(port=0)

    def test_base_url(self):
        svc = make_svc(host="api.internal", port=9000)
        assert svc.base_url == "http://api.internal:9000"

    def test_https_protocol(self):
        svc = make_svc(protocol=Protocol.HTTPS, port=443)
        assert "https" in svc.base_url

    def test_health_url(self):
        svc = make_svc(host="db", port=5432, health_endpoint="/ping")
        assert svc.health_url == "http://db:5432/ping"

    def test_empty_name(self):
        with pytest.raises(ValueError):
            make_svc(name="")

    def test_invalid_weight(self):
        with pytest.raises(ValueError):
            make_svc(weight=0)


class TestCircuitBreakerState:
    def test_initial_closed(self):
        cb = CircuitBreakerState(service="api")
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold(self):
        policy = TrafficPolicy(circuit_breaker_threshold=3)
        cb = CircuitBreakerState(service="api")
        for _ in range(3):
            cb.record_failure(policy)
        assert cb.state == CircuitState.OPEN

    def test_is_open_returns_false_when_closed(self):
        policy = TrafficPolicy()
        cb = CircuitBreakerState(service="api")
        assert cb.is_open(policy) is False

    def test_record_success_resets(self):
        policy = TrafficPolicy(circuit_breaker_threshold=2)
        cb = CircuitBreakerState(service="api")
        cb.record_failure(policy)
        cb.record_failure(policy)
        assert cb.state == CircuitState.OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0


class TestRegisterService:
    def test_register(self, tmp_path):
        db = make_db(tmp_path)
        svc = make_svc(name="auth", host="auth-svc", port=8001)
        sid = register_service(svc, db)
        assert sid > 0

    def test_upsert_on_re_register(self, tmp_path):
        db = make_db(tmp_path)
        svc = make_svc(name="auth", host="auth-svc", port=8001)
        register_service(svc, db)
        register_service(svc, db)
        services = list_services(db=db)
        assert len(services) == 1

    def test_get_service(self, tmp_path):
        db = make_db(tmp_path)
        svc = make_svc(name="orders", host="orders-svc", port=8002)
        register_service(svc, db)
        fetched = get_service("orders", db)
        assert fetched is not None
        assert fetched.host == "orders-svc"

    def test_deregister(self, tmp_path):
        db = make_db(tmp_path)
        svc = make_svc(name="payments")
        register_service(svc, db)
        result = deregister_service("payments", db)
        assert result is True
        assert get_service("payments", db) is None

    def test_deregister_nonexistent(self, tmp_path):
        db = make_db(tmp_path)
        result = deregister_service("ghost", db)
        assert result is False


class TestRouteRequest:
    def test_successful_route(self, tmp_path):
        db = make_db(tmp_path)
        register_service(make_svc(name="frontend", host="fe", port=3000), db)
        register_service(make_svc(name="backend", host="be", port=8080), db)
        result = route_request("frontend", "backend", "/api", db=db)
        assert result.success
        assert result.source == "frontend"
        assert result.destination == "backend"

    def test_route_to_nonexistent_fails(self, tmp_path):
        db = make_db(tmp_path)
        result = route_request("a", "nonexistent", db=db)
        assert not result.success
        assert "not found" in result.error

    def test_route_log_recorded(self, tmp_path):
        db = make_db(tmp_path)
        register_service(make_svc(name="svc-a", host="a", port=8080), db)
        register_service(make_svc(name="svc-b", host="b", port=8081), db)
        route_request("svc-a", "svc-b", db=db)
        route_request("svc-a", "svc-b", db=db)
        graph = get_service_graph(db)
        assert "svc-a" in graph
        assert "svc-b" in graph["svc-a"]
        assert graph["svc-a"]["svc-b"]["calls"] == 2


class TestServiceGraph:
    def test_empty_graph(self, tmp_path):
        db = make_db(tmp_path)
        graph = get_service_graph(db)
        assert graph == {}

    def test_traffic_stats(self, tmp_path):
        db = make_db(tmp_path)
        register_service(make_svc(name="a", host="a", port=80), db)
        register_service(make_svc(name="b", host="b", port=81), db)
        for _ in range(5):
            route_request("a", "b", db=db)
        stats = get_traffic_stats(db)
        assert stats["total_requests"] == 5
        assert stats["avg_latency_ms"] >= 0
