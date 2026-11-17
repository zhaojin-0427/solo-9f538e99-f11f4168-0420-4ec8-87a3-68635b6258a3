"""测试夹具：每个用例使用独立的临时 DATA_DIR，互不污染。"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

# 必须在导入 app.config 之前指定数据目录
_TMP_BASE = Path(tempfile.mkdtemp(prefix="wq-tests-"))
os.environ["DATA_DIR"] = str(_TMP_BASE / "data")

from app.config import DATA_DIR, SHARD_DIR  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(autouse=True)
def fresh_data_dir():
    """每个用例前清空数据目录，保证字段版/快照从空态开始。"""
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    SHARD_DIR.mkdir(parents=True, exist_ok=True)
    yield
    shutil.rmtree(DATA_DIR, ignore_errors=True)


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


def write_parquet(path: Path, columns: dict[str, list], types: dict[str, pa.DataType] | None = None) -> Path:
    """按给定列构造 Parquet；types 可强制列类型（如 timestamp / string）。"""
    types = types or {}
    arrays = {}
    for name, values in columns.items():
        arrays[name] = pa.array(values, type=types.get(name))
    pq.write_table(pa.table(arrays), path, compression="zstd")
    return path


def upload(client, parquet_path: Path, version: int):
    with open(parquet_path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (parquet_path.name, fh.read(), "application/vnd.apache.parquet")},
        )


@pytest.fixture()
def make_parquet(tmp_path):
    def _make(name: str, columns, types=None):
        return write_parquet(tmp_path / name, columns, types)

    return _make


@pytest.fixture()
def upload_parquet(upload_fn=upload):
    return upload
