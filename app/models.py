"""Pydantic 响应/请求模型与统一错误类型。"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator


class ConflictError(HTTPException):
    """业务规则冲突（409）：请求本身合法，但违反数仓不变量。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=409, detail=detail)


class NotFound(HTTPException):
    """资源不存在（404）。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=404, detail=detail)


class IndicatorOut(BaseModel):
    indicator_id: int = Field(..., description="指标稳定身份 ID；改名不变，同名复用会得到新 ID")
    name: str = Field(..., description="当前指标名")
    active: bool


class SchemaOut(BaseModel):
    current_version: int = Field(..., description="当前字段版号")
    fixed_columns: List[str] = Field(..., description="固定字段")
    indicators: List[IndicatorOut] = Field(..., description="当前存续指标（按指标 ID 排序）")


class VersionIndicatorOut(BaseModel):
    indicator_id: int
    name: str
    active: bool


class VersionOut(BaseModel):
    version_id: int
    created_at: str
    change_note: str
    indicators: List[VersionIndicatorOut]


class RenameRequest(BaseModel):
    old_name: str = Field(..., min_length=1, description="当前指标名（例：浓度）")
    new_name: str = Field(..., min_length=1, description="新指标名（例：硝酸盐）")


class CreateIndicatorRequest(BaseModel):
    name: str = Field(..., min_length=1, description="新指标名；若与历史指标同名则视为全新指标，不继承旧值")


class IndicatorChangeResponse(BaseModel):
    indicator_id: int
    name: str
    active: bool
    new_version: int = Field(..., description="本次变更产生的字段版号")


class UploadResponse(BaseModel):
    shard_id: int
    schema_version: int = Field(..., description="本次上传指定并裁决所用的字段版")
    row_count: int
    stored_columns: List[str] = Field(..., description="分片实际落盘的指标身份列")
    file_path: str


class SnapshotMeta(BaseModel):
    snapshot_id: int
    created_at: str
    indicator_names: List[str]
    shard_ids: List[int]


class RowPage(BaseModel):
    snapshot_id: int
    snapshot_created: bool = Field(..., description="本次调用是否新建了快照（首次查询为 true）")
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    fixed_columns: List[str]
    indicator_names: List[str] = Field(..., description="快照固化的当前指标名；旧分片没有的指标为 null")
    rows: List[dict]


# ---------------------------------------------------------------------------
# 核对发布
# ---------------------------------------------------------------------------

class ReferenceValueItem(BaseModel):
    """一个交接对照值及其绝对容差。"""
    value: float = Field(..., description="交接对照值（有限浮点，不接受 NaN/Infinity）")
    tolerance: float = Field(..., ge=0, description="该对照值的绝对容差（非负有限浮点）")
    source: Optional[str] = Field(
        None, max_length=200, description="对照值来源（如移交单号/分析组编号），可空"
    )

    @field_validator("value")
    @classmethod
    def _value_must_be_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("对照值必须是有限浮点，不接受 NaN/Infinity")
        return v

    @field_validator("tolerance")
    @classmethod
    def _tolerance_must_be_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("容差必须是有限浮点，不接受 NaN/Infinity")
        return v


class CheckPublishRequest(BaseModel):
    snapshot_id: int = Field(..., description="已有快照号；只在该快照固化的分片内取实测")
    station: str = Field(..., min_length=1, description="站点标识（非空字符串）")
    sampled_at: datetime = Field(..., description="UTC 采样时刻；带偏移量按 UTC 换算，无时区按 UTC 处理")
    indicator_id: int = Field(..., ge=1, description="指标稳定身份 ID（改名不变，同名复用为新 ID）")
    references: List[ReferenceValueItem] = Field(
        ..., min_length=1, max_length=20, description="1~20 个交接对照值，各自带绝对容差"
    )

    @field_validator("station")
    @classmethod
    def _station_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("站点标识不能为空字符串")
        return v

    def normalized_sampled_at(self) -> datetime:
        """统一为 UTC 微秒精度的 tz-aware datetime。"""
        dt = self.sampled_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=dt.microsecond)


class PublishedPair(BaseModel):
    pair_position: int
    measured_value: float
    measured_shard_id: int = Field(..., description="实测值来源分片 ID")
    measured_row_index: int = Field(..., description="实测值在分片 Parquet 内的物理行号（0 起）")
    reference_value: float
    tolerance: float
    abs_diff: float
    reference_source: Optional[str] = None


class PublishSuccessResponse(BaseModel):
    published: bool = True
    publication_id: int
    snapshot_id: int
    station: str
    sampled_at: str = Field(..., description="归一化后的 UTC 时刻（微秒精度，Z 后缀）")
    indicator_id: int
    indicator_name: str = Field(..., description="发布时刻固化的指标名；后续改名不影响已发布结果")
    pair_count: int
    total_abs_diff: float = Field(..., description="本次配对的绝对差总和（字典序第二目标）")
    pairs: List[PublishedPair]


class UnpairedMeasured(BaseModel):
    value: float
    shard_id: int
    row_index: int


class NullMeasured(BaseModel):
    shard_id: int
    row_index: int


class MissingReference(BaseModel):
    value: float
    tolerance: float
    source: Optional[str] = None


class CheckFailureDetail(BaseModel):
    """配对失败时的结构化原因（HTTP 409 的 detail）。"""
    reason: str
    snapshot_id: int
    station: str
    sampled_at: str
    indicator_id: int
    measured_count: int = Field(..., description="同键非 NULL 实测条数")
    null_measured_count: int = Field(0, description="同键但指标值为 NULL、无法配对的实测条数")
    reference_count: int
    matched_pairs: int = Field(..., description="容差内最多能配成的对数（第一目标）")
    extra_measured: List[UnpairedMeasured] = Field(
        ..., description="多余实测：找不到交接值配对的实测及其来源"
    )
    null_measured: List[NullMeasured] = Field(
        ..., description="NULL 实测：同键但值为 NULL，不可配对"
    )
    missing_references: List[MissingReference] = Field(
        ..., description="缺失：找不到实测配对的交接对照值及其来源"
    )
    best_pairs: List[PublishedPair] = Field(
        ..., description="按“先最多配对、再最小绝对差总和”得到的尽力配对（仅供核对，不发布）"
    )


class PublicationSummary(BaseModel):
    publication_id: int
    snapshot_id: int
    station: str
    sampled_at: str
    indicator_id: int
    indicator_name: str
    pair_count: int
    total_abs_diff: float
    created_at: str


class PublicationListResponse(BaseModel):
    count: int
    publications: List[PublicationSummary]


class PublicationDetail(BaseModel):
    publication_id: int
    snapshot_id: int
    station: str
    sampled_at: str
    indicator_id: int
    indicator_name: str
    pair_count: int
    total_abs_diff: float
    created_at: str
    pairs: List[PublishedPair]
