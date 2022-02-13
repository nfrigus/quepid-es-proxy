from asyncio import Future
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from asynctest import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from quepid_es_proxy import metrics
from quepid_es_proxy.metrics import (
    BaseMetricsManager,
    BenchmarkRecord,
    ElasticSearchMetricsManager,
    EndpointMetric,
    add_metrics_app,
)


class MetricManagerMock(BaseMetricsManager):
    def store(self, data: BenchmarkRecord):
        pass

    async def retrieve(self, interval) -> list:
        return ["MetricManagerMock response"]


manager = MagicMock(wraps=MetricManagerMock())
app = FastAPI()
add_metrics_app(
    app,
    measure_routes=[
        "units.test_metrics.dymic_route",
        "units.test_metrics.simple_route",
    ],
    manager=manager,
)


@app.get("/measured-dynamic/{dynamic}")
def dymic_route() -> None:
    pass


@app.get("/measured-simple")
def simple_route() -> None:
    pass


@app.get("/unmeasured")
def excluded_route() -> None:
    pass


client = TestClient(app)


@pytest.mark.parametrize(
    "request_query,expected_query",
    [("", "24h"), ("?interval=1h", "1h"), ("?interval=error", "error")],
)
def test_metrics_endpoint(request_query, expected_query):
    manager.reset_mock()
    response = client.get(f"/metrics/{request_query}")

    manager.retrieve.assert_called_once_with(expected_query)

    assert response.status_code == 200
    assert response.json() == ["MetricManagerMock response"]


def test_dynamic_route_measured() -> None:
    manager.reset_mock()
    client.get("/measured-dynamic/dynamic")
    manager.store.assert_called_once()


def test_simple_route_measured() -> None:
    manager.reset_mock()
    client.get("/measured-simple")
    manager.store.assert_called_once()


def test_exclude_route_measure() -> None:
    manager.reset_mock()
    client.get("/unmeasured")
    assert not manager.store.called


def setup_es_manager():
    client = AsyncMock()
    future = Future()
    future.set_result(client)

    manager = ElasticSearchMetricsManager(client=future)

    return client, manager


class DatetimeMock:
    _now = datetime.now()

    @classmethod
    def now(cls):
        return cls._now


async def test_es_manager_store():
    client, manager = setup_es_manager()
    record = BenchmarkRecord("test-endpoint", 42)

    with patch.object(metrics, "datetime", wraps=DatetimeMock):
        await manager.store(record)

    assert client.index.call_count == 3
    client.index.assert_has_calls(
        [
            call("quepid_es_proxy.metrics", {}),
            call(
                "quepid_es_proxy.metrics",
                ElasticSearchMetricsManager.mapping,
                doc_type="_mapping",
            ),
            call(
                "quepid_es_proxy.metrics",
                {
                    "endpoint": "test-endpoint",
                    "wall_ms": 42,
                    "timestamp": DatetimeMock._now.isoformat(),
                },
            ),
        ]
    )


async def test_es_manager_retrieve():
    client, manager = setup_es_manager()
    client.search.return_value = {
        "aggregations": {
            "endpoints": {
                "buckets": [
                    {
                        "key": "test-endpoint",
                        "average_response_time_ms": {"value": 21},
                        "doc_count": 777,
                    }
                ]
            }
        }
    }
    result = await manager.retrieve()
    assert type(result) == list
    assert len(result) == 1
    assert type(result[0]) == EndpointMetric
    assert result[0].average_response_time == 21
    assert result[0].endpoint == "test-endpoint"
    assert result[0].interval == "24h"
    assert result[0].requests_count == 777


async def test_base_manager():
    manager = BaseMetricsManager()
    manager.store(MagicMock())
    await manager.retrieve("1h")
