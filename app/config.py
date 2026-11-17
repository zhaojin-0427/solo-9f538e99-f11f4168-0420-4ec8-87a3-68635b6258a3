"""全局配置与常量。

数据全部落在 ``DATA_DIR`` 下：
* ``warehouse.db`` —— SQLite 元数据库（字段身份、字段版、分片、快照）
* ``shards/``      —— 每次上传落盘的 Parquet 分片（以指标身份 ID 为列名）
"""
from __future__ import annotations

import os
from pathlib import Path

# 固定字段（每次化验都必须存在）
STATION_COLUMN = "station"
SAMPLED_AT_COLUMN = "sampled_at"
FIXED_COLUMNS = (STATION_COLUMN, SAMPLED_AT_COLUMN)

# 同一时刻最多允许存在的可空浮点指标数量
MAX_INDICATORS = 8

# 指标身份列在 Parquet 分片中的列名前缀，避免与固定字段冲突
INDICATOR_COLUMN_PREFIX = "i"

# 保留名：指标名不允许与固定字段、健康检查路径冲突
RESERVED_NAMES = frozenset(FIXED_COLUMNS)

DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data")))
DB_PATH = DATA_DIR / "warehouse.db"
SHARD_DIR = DATA_DIR / "shards"
DB_URL = f"file:{DB_PATH}?uri=true"
