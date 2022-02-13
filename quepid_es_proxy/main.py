from typing import List, Optional, Union

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .auth import basic_auth
from .elasticsearch import connection, executor
from .metrics import ElasticSearchMetricsManager, add_metrics_app

app = FastAPI()

# Replace "*" to the list of your origins, e.g.
# origins = ["quepid.yourcompany.com", "localhost:8080"]
origins = "*"

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

metrics_manager = ElasticSearchMetricsManager(client=connection.get_connection())

add_metrics_app(
    app,
    measure_routes=[
        "quepid_es_proxy.main.explain_missing_documents",
        "quepid_es_proxy.main.explain",
        "quepid_es_proxy.main.search_proxy",
    ],
    manager=metrics_manager,
)


@app.get("/healthcheck")
async def root():
    """Health check"""
    return {"status": "OK"}


class ProxyRequst(BaseModel):
    explain: bool
    from_: int
    size: int
    source: Union[str, List[str], None]
    query: Optional[dict]

    class Config:
        fields = {"from_": "from", "source": "_source"}


@app.post("/{index_name}")
async def search_proxy(
    index_name: str, body: ProxyRequst, username: str = Depends(basic_auth)
) -> dict:
    result = await executor.search(
        index_name,
        body.from_,
        body.size,
        body.explain,
        body.source,
        {"query": body.query} if body.query else None,
        None,
    )
    return result


@app.get("/{index_name}")
async def explain_missing_documents(
    index_name: str,
    _source: str,
    q: str,
    size: int,
    username: str = Depends(basic_auth),
) -> dict:
    result = await executor.search(
        index_name,
        0,
        size,
        False,
        _source,
        None,
        q,
    )
    return result


@app.post("/{index_name}/_doc/{doc_id}/_explain")
async def explain(
    index_name: str,
    doc_id: str,
    query: dict,
    username: str = Depends(basic_auth),
) -> dict:
    return await executor.explain(index_name, doc_id, query)
