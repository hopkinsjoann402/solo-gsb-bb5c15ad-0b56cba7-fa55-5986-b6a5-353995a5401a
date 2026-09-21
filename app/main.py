"""FastAPI 应用：实验室样品分装谱系 API。"""
from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse

from . import service
from .db import get_db, init_db
from .schemas import (
    BatchPreviewOut,
    BatchTransferIn,
    BatchTransferOut,
    ContainerIn,
    ContainerOut,
    ContainerStatusOut,
    SampleIn,
    SampleOut,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 容器启动时建库（首次运行则创建文件与表）
    init_db()
    yield


app = FastAPI(
    title="实验室样品分装谱系 API",
    description=(
        "登记带体积的初始样品后，批量提交依次发生的容器间转移；"
        "每次按源容器当时的混合比例传播全部祖先来源（Decimal 精确计算）。"
        "支持 /transfers/preview 预演：与真实提交同一套校验与分摊，但不落库。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(service.LineageError)
async def lineage_error_handler(request: Request, exc: service.LineageError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


@app.get("/health", tags=["meta"], summary="健康检查")
def health() -> dict:
    return {"status": "ok"}


@app.post(
    "/samples",
    response_model=SampleOut,
    status_code=201,
    tags=["registry"],
    summary="登记带体积的初始样品",
)
def create_sample(body: SampleIn, conn: sqlite3.Connection = Depends(get_db)) -> dict:
    volume = service.register_initial_sample(conn, body.sample_id, body.volume)
    return {"sample_id": body.sample_id, "volume": volume}


@app.post(
    "/containers",
    response_model=ContainerOut,
    status_code=201,
    tags=["registry"],
    summary="登记空容器",
)
def create_container(
    body: ContainerIn, conn: sqlite3.Connection = Depends(get_db)
) -> dict:
    service.register_empty_container(conn, body.container_id)
    return {"container_id": body.container_id}


@app.post(
    "/transfers/batch",
    response_model=BatchTransferOut,
    tags=["transfers"],
    summary="批量提交依次发生的转移（整批原子，支持可选幂等键）",
)
def batch_transfers(
    body: BatchTransferIn,
    conn: sqlite3.Connection = Depends(get_db),
    idempotency_key_header: str | None = Header(
        default=None,
        alias="Idempotency-Key",
        max_length=255,
        description=(
            "可选幂等键；等价于请求体中的 idempotency_key。二者同时给出且"
            "不一致时返回 422 invalid_idempotency_key。"
        ),
    ),
) -> JSONResponse | dict:
    triples = [(t.source, t.destination, t.volume) for t in body.transfers]

    # 幂等键可经请求体字段或 Idempotency-Key 头提供（去首尾空白）
    header_key = (
        idempotency_key_header.strip() if idempotency_key_header is not None else None
    )
    idem_key = body.idempotency_key
    if idem_key == "":
        raise service.LineageError(
            422, "invalid_idempotency_key", "idempotency_key 不能为空白字符串"
        )
    if header_key == "":
        raise service.LineageError(
            422, "invalid_idempotency_key", "Idempotency-Key 不能为空白字符串"
        )
    if header_key is not None:
        if idem_key is not None and idem_key != header_key:
            raise service.LineageError(
                422,
                "invalid_idempotency_key",
                (
                    "请求体 idempotency_key 与 Idempotency-Key 请求头不一致："
                    "请只保留一处，或让二者取值相同"
                ),
            )
        idem_key = header_key

    if idem_key is None:
        # 未携带幂等键：完全保持原有行为
        applied, versions = service.apply_transfers(
            conn, triples, body.expected_versions
        )
        return {
            "applied": applied,
            "message": f"已依次提交 {applied} 条转移",
            "versions": versions,
        }

    # 指纹基于“除 idempotency_key 外的请求体”（经 Pydantic 校验规范化后的内容）
    fingerprint_payload = body.model_dump(
        exclude={"idempotency_key"}, mode="json"
    )
    fingerprint = service.fingerprint_request(fingerprint_payload)
    result, replayed = service.apply_transfers_idempotent(
        conn,
        triples,
        body.expected_versions,
        idem_key,
        fingerprint,
    )
    if replayed:
        # 重放首次成功时持久化的完整响应，不再经 response_model 重算
        return JSONResponse(status_code=200, content=result)
    return result


@app.post(
    "/transfers/preview",
    response_model=BatchPreviewOut,
    tags=["transfers"],
    summary="预演批量转移（不落库）",
)
def preview_transfers(
    body: BatchTransferIn, conn: sqlite3.Connection = Depends(get_db)
) -> dict:
    triples = [(t.source, t.destination, t.volume) for t in body.transfers]
    return service.preview_transfers(conn, triples, body.expected_versions)


@app.get(
    "/containers/{container_id}",
    response_model=ContainerStatusOut,
    tags=["query"],
    summary="查询容器当前体积及来源占比",
)
def container_status(
    container_id: str, conn: sqlite3.Connection = Depends(get_db)
) -> dict:
    return service.get_container_status(conn, container_id)
