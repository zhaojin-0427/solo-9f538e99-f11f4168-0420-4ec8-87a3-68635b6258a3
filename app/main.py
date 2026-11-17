"""FastAPI 入口：化验表字段管理、Parquet 上传与快照分页查询。

并发模型
========
进程内一把可重入锁 ``write_lock`` 保护所有变更（字段改名/删除/新增、上传、
快照创建）。锁内再以 ``BEGIN IMMEDIATE`` 事务裁决，保证字段变更与上传交错时
依据同一个已提交字段状态；任何失败都回滚，不开放半份结果。
"""
from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import analysis, catalog, ingest, query, verification
from .config import FIXED_COLUMNS, MAX_INDICATORS
from .db import connect, immediate_transaction, init_db
from .models import (
    CheckPublishRequest,
    CreateIndicatorRequest,
    IndicatorChangeResponse,
    IndicatorOut,
    NotFound,
    PublicationDetail,
    PublicationListResponse,
    PublishSuccessResponse,
    RenameRequest,
    RowPage,
    SchemaOut,
    SnapshotMeta,
    UploadResponse,
    VersionOut,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    app.state.write_lock = threading.RLock()
    yield


app = FastAPI(
    title="水质数仓化验表 API",
    version="1.0.0",
    description=(
        "固定站点 + UTC 采样时刻，至多 8 个可空浮点指标的化验表服务。\n\n"
        "- 指标有稳定身份，改名不换身份；同名复用是全新指标，不继承旧值。\n"
        "- 上传 Parquet 必须指定字段版；含已删指标或未知列整次拒绝。\n"
        "- 首次查询固化字段与分片并返回快照号，之后任何变更/上传/重启都不影响该快照。\n"
        "- 核对发布：对指定快照同键实测与 1~20 个交接对照值做一对一配对，"
        "全部配上才整组发布；分析读取只提供已发布记录及其来源。"
    ),
    lifespan=lifespan,
)


def get_lock(request: Request) -> threading.RLock:
    return request.app.state.write_lock


def get_conn():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@app.exception_handler(NotFound)
async def not_found_handler(_request: Request, exc: NotFound):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


def _sanitize_json(obj):
    """递归把 NaN/Infinity 及不可 JSON 编码的对象转为可编码形式，保证错误体是合法 JSON。"""
    import math

    if isinstance(obj, float):
        if math.isnan(obj):
            return "NaN"
        if math.isinf(obj):
            return "Infinity" if obj > 0 else "-Infinity"
        return obj
    if isinstance(obj, dict):
        return {str(k): _sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_json(v) for v in obj]
    if isinstance(obj, (str, int, bool)) or obj is None:
        return obj
    # Pydantic 错误 ctx 里可能带原始异常等非 JSON 原生对象
    return str(obj)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_request: Request, exc: RequestValidationError):
    # 默认处理器会把请求里的 NaN/Infinity 原样回显，标准 JSON 无法编码；清洗后再返回
    return JSONResponse(status_code=422, content={"detail": _sanitize_json(exc.errors())})


@app.get("/health", tags=["运维"], summary="健康检查")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# 字段目录
# ---------------------------------------------------------------------------

@app.get("/schema", response_model=SchemaOut, tags=["字段管理"], summary="查询当前字段状态")
def get_schema(conn=Depends(get_conn)):
    indicators = [
        IndicatorOut(indicator_id=r["indicator_id"], name=r["name"], active=bool(r["active"]))
        for r in catalog.active_indicators(conn)
    ]
    return SchemaOut(
        current_version=catalog.latest_version(conn),
        fixed_columns=list(FIXED_COLUMNS),
        indicators=indicators,
    )


@app.get(
    "/schema/versions/{version_id}",
    response_model=VersionOut,
    tags=["字段管理"],
    summary="查询某个字段版的不可变映射（含已删指标）",
)
def get_version(version_id: int, conn=Depends(get_conn)):
    rows = catalog.version_mapping(conn, version_id)
    note = catalog.version_note(conn, version_id)  # 不存在会抛 404
    created = conn.execute(
        "SELECT created_at FROM schema_versions WHERE version_id = ?", (version_id,)
    ).fetchone()
    return VersionOut(
        version_id=version_id,
        created_at=created["created_at"],
        change_note=note,
        indicators=[
            {"indicator_id": r["indicator_id"], "name": r["name"], "active": bool(r["active"])}
            for r in rows
        ],
    )


@app.post(
    "/schema/indicators",
    response_model=IndicatorChangeResponse,
    status_code=201,
    tags=["字段管理"],
    summary="新增指标（同名复用为全新身份，不继承旧值）",
)
def create_indicator(
    payload: CreateIndicatorRequest,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    with lock, immediate_transaction(conn):
        indicator_id, name, new_version = catalog.create_indicator(conn, payload.name)
    return IndicatorChangeResponse(
        indicator_id=indicator_id, name=name, active=True, new_version=new_version
    )


@app.post(
    "/schema/indicators/rename",
    response_model=IndicatorChangeResponse,
    tags=["字段管理"],
    summary="指标改名（身份不变，产生新字段版）",
)
def rename_indicator(
    payload: RenameRequest,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    with lock, immediate_transaction(conn):
        indicator_id, name, new_version = catalog.rename_indicator(
            conn, payload.old_name, payload.new_name
        )
    return IndicatorChangeResponse(
        indicator_id=indicator_id, name=name, active=True, new_version=new_version
    )


@app.delete(
    "/schema/indicators/{name}",
    response_model=IndicatorChangeResponse,
    tags=["字段管理"],
    summary="删除指标（历史分片保留；再上传含该指标的文件会被整次拒绝）",
)
def delete_indicator(
    name: str,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    with lock, immediate_transaction(conn):
        indicator_id, new_version = catalog.delete_indicator(conn, name)
    return IndicatorChangeResponse(
        indicator_id=indicator_id, name=name, active=False, new_version=new_version
    )


# ---------------------------------------------------------------------------
# Parquet 上传
# ---------------------------------------------------------------------------

@app.post(
    "/ingest/parquet",
    response_model=UploadResponse,
    tags=["数据接入"],
    summary="上传 Parquet 分片（须指定字段版）",
)
async def upload_parquet(
    request: Request,
    schema_version: int = Form(..., description="该文件遵循的字段版号"),
    file: UploadFile = File(..., description="Parquet 文件，须含 station、sampled_at 两列"),
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse(status_code=409, content={"detail": "上传文件为空"})
    with lock, immediate_transaction(conn):
        result = ingest.ingest_upload(conn, file_bytes, schema_version, file.filename or "unknown.parquet")
    return UploadResponse(**result)


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

@app.get(
    "/rows",
    response_model=RowPage,
    tags=["查询"],
    summary="分页查询化验数据（首次不带 snapshot_id 即创建快照）",
)
def get_rows(
    request: Request,
    snapshot_id: Optional[int] = Query(None, description="快照号；首次查询留空，服务端创建并返回"),
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(100, ge=1, le=10000, description="每页行数，1~10000"),
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    # 快照创建是写操作，纳入同一把锁；纯翻页（带 snapshot_id）也走锁以获得确定视图
    with lock, immediate_transaction(conn):
        result = query.query_page(conn, snapshot_id, page, page_size)
    return result


@app.get(
    "/snapshots/{snapshot_id}",
    response_model=SnapshotMeta,
    tags=["查询"],
    summary="查看快照固化的字段与分片",
)
def get_snapshot(snapshot_id: int, conn=Depends(get_conn)):
    meta = query.get_snapshot_meta(conn, snapshot_id)
    return SnapshotMeta(
        snapshot_id=meta["snapshot_id"],
        created_at=meta["created_at"],
        indicator_names=meta["indicator_names"],
        shard_ids=meta["shard_ids"],
    )


# ---------------------------------------------------------------------------
# 核对发布
# ---------------------------------------------------------------------------

@app.post(
    "/verification/publish",
    response_model=PublishSuccessResponse,
    tags=["核对发布"],
    summary="提交交接对照值核对；同键实测全部一对一配上才整组发布",
    responses={
        409: {"description": "配对失败（缺失/多余/NULL/超容差/超 20 条/重复发布），detail 为结构化原因"},
        404: {"description": "快照或指标身份不存在"},
        422: {"description": "请求体校验失败（如对照值数量不在 1~20、容差为负、NaN/Infinity）"},
    },
)
def publish_verification(
    payload: CheckPublishRequest,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    # 与字段变更/上传共用同一把写锁 + 立即事务：发布要么整组可见，要么完全不可见
    with lock, immediate_transaction(conn):
        result = verification.check_and_publish(conn, payload)
    return result


# ---------------------------------------------------------------------------
# 分析读取（只读已发布记录）
# ---------------------------------------------------------------------------

@app.get(
    "/analysis/published",
    response_model=PublicationListResponse,
    tags=["分析读取"],
    summary="列出已发布的核对记录（只读，未发布的原始实测不出现）",
)
def list_published(
    snapshot_id: Optional[int] = Query(None, description="可选：按快照号过滤"),
    station: Optional[str] = Query(None, description="可选：按站点过滤"),
    indicator_id: Optional[int] = Query(None, description="可选：按指标身份过滤"),
    conn=Depends(get_conn),
):
    return analysis.list_publications(conn, snapshot_id, station, indicator_id)


@app.get(
    "/analysis/published/{publication_id}",
    response_model=PublicationDetail,
    tags=["分析读取"],
    summary="读取单条已发布记录的配对明细与双方来源",
)
def get_published(publication_id: int, conn=Depends(get_conn)):
    return analysis.get_publication_detail(conn, publication_id)


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "water-quality-warehouse",
        "docs": "/docs",
        "max_indicators": MAX_INDICATORS,
        "fixed_columns": list(FIXED_COLUMNS),
    }
