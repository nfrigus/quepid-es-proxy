from asyncio import create_task
from contextlib import AbstractContextManager, nullcontext
from datetime import datetime
from time import time
from typing import Any, Callable, ContextManager

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from starlette.middleware.base import RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match, Route


class BenchmarkRecord:
    def __init__(self, name: str, wall_ms: float):
        self.name = name
        self.wall_ms = wall_ms


RequestMeasured = Callable[[BenchmarkRecord], None]


class Benchmark(AbstractContextManager):
    """
    Capture performance metrics with context manager.
    Resulting message is send with done callback.
    """

    def __init__(
        self,
        name: str,
        done: RequestMeasured,
    ) -> None:
        self.name = name
        self.done = done

        self.start_time: float = 0

    def __enter__(self) -> "Benchmark":
        self.start_time = time()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        record = self.take_record()

        self.done(record)

    def take_record(self) -> BenchmarkRecord:
        end_time = time()

        return BenchmarkRecord(
            name=self.name, wall_ms=1000 * (end_time - self.start_time)
        )


class EndpointMetric:
    def __init__(self, endpoint, average_response_time, interval, requests_count):
        self.average_response_time = average_response_time
        self.endpoint = endpoint
        self.interval = interval
        self.requests_count = requests_count


class BaseMetricsManager:
    """
    Base data management class for Metrics data.
    """

    def store(self, data: BenchmarkRecord):
        pass

    async def retrieve(self, interval) -> list:
        pass


class ElasticSearchMetricsManager(BaseMetricsManager):
    """
    Metrics data manager implementation for ElastiSearch storage
    """

    mapping = {
        "properties": {
            "timestamp": {"type": "date"},
            "endpoint": {"type": "keyword"},
            "wall_ms": {"type": "float"},
        },
    }

    def __init__(self, client, index: str = "quepid_es_proxy.metrics"):
        self.client = client
        self.index = index
        self.es: AsyncElasticsearch = None
        self.mapping_requested = None

    async def setup(self):
        if self.es is None:
            self.es = await self.client
        if self.mapping_requested is None:
            self.mapping_requested = await self.create_mapping()

    async def create_mapping(self):
        await self.es.index(self.index, {})
        return await self.es.index(
            self.index, ElasticSearchMetricsManager.mapping, doc_type="_mapping"
        )

    async def retrieve(self, interval: str = None) -> list:
        if interval not in ["1h", "8h", "24h"]:
            interval = "24h"

        await self.setup()
        response = await self.es.search(
            index=self.index,
            body={
                "size": 0,
                "query": {"range": {"timestamp": {"gte": f"now-{interval}"}}},
                "aggs": {
                    "endpoints": {
                        "aggs": {
                            "average_response_time_ms": {
                                "avg": {"field": "wall_ms"},
                            },
                        },
                        "terms": {"field": "endpoint"},
                    },
                },
            },
        )
        return [
            EndpointMetric(
                endpoint=record.get("key"),
                average_response_time=record.get("average_response_time_ms").get(
                    "value"
                ),
                interval=interval,
                requests_count=record.get("doc_count"),
            )
            for record in response.get("aggregations").get("endpoints").get("buckets")
        ]

    def store(self, data: BenchmarkRecord):
        return create_task(self._store(data))

    async def _store(self, data: BenchmarkRecord):
        await self.setup()
        await self.es.index(
            self.index,
            {
                "endpoint": data.name,
                "wall_ms": data.wall_ms,
                "timestamp": datetime.now().isoformat(),
            },
        )


class RequestMetricsHandler:
    """
    Request metrics acquisition manager - identifies routes, match them against routes measurement list
    and apply benchmark timer for request duration management.
    """

    def __init__(
        self,
        app: FastAPI,
        measure_routes: list,
        request_measured: RequestMeasured,
    ):
        self.app = app
        self.measure_routes = measure_routes
        self.request_measured = request_measured

    async def __call__(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        with self.get_route_handler(request):
            response = await call_next(request)

        return response

    def is_route_measurable(self, route_name: str) -> bool:
        return route_name in self.measure_routes

    def get_route_handler(self, request: Request) -> ContextManager:
        name = self.get_route_name(request)

        return (
            Benchmark(name, done=self.request_measured)
            if self.is_route_measurable(name)
            else nullcontext()
        )

    def get_matching_route(self, request: Request) -> Route:
        for r in self.app.router.routes:
            if r.matches(request.scope)[0] == Match.FULL:
                return r

    def get_route_name(self, request: Request) -> str:
        route = self.get_matching_route(request)
        name = ""

        if hasattr(route, "endpoint") and hasattr(route, "name"):
            name = f"{route.endpoint.__module__}.{route.name}"

        return name


def add_metrics_app(
    app: FastAPI,
    manager: BaseMetricsManager,
    measure_routes: list,
):
    """
    Register metrics module. Enables request metrics measurement with timing middleware
    and provide collected data via /metrics/ endpoint.

    Arguments:
        app - FastAPI application for integration
        manager - metrics data manager to persist and access collected statistics
        measure_routes - list of routes to apply measurement
    """
    metrics_app = FastAPI()
    handler = RequestMetricsHandler(
        app, request_measured=manager.store, measure_routes=measure_routes
    )

    app.mount(path="/metrics", app=metrics_app, name="metrics")

    @app.middleware("http")
    async def timing_middleware(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        return await handler(request, call_next)

    @metrics_app.get("/")
    async def get_metrics(interval: str = "24h"):
        """
        Retrieve basic metrics for measured endpoints.
        Query parameters:
            interval - dataset timerange duration (supported values: 1h, 8h, 24h)
        """
        return await manager.retrieve(interval)
