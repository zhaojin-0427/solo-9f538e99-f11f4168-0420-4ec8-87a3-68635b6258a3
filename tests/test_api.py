"""关键功能自测：改名不串列、字段版裁决、快照隔离、同名复用、分页等。"""
from __future__ import annotations

import threading

import pyarrow as pa
import pyarrow.parquet as pq

from app.config import DATA_DIR
from app.db import connect, init_db
from app.main import app
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# 主故事：浓度 -> 硝酸盐，再用“浓度”接入磷酸盐，历史值不串列
# ---------------------------------------------------------------------------

def test_full_story_rename_then_reuse_name_no_cross_wiring(client, make_parquet, tmp_path):
    # 1) 新增指标“浓度”（字段版 2）
    r = client.post("/schema/indicators", json={"name": "浓度"})
    assert r.status_code == 201, r.text
    v2 = r.json()["new_version"]
    nitrate_id = r.json()["indicator_id"]
    assert v2 == 2

    # 2) 用字段版 2 上传历史分片：站点 A，浓度=11.5
    f1 = make_parquet("hist.parquet", {
        "station": ["A"],
        "sampled_at": ["2026-01-01T00:00:00Z"],
        "浓度": [11.5],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, f1, v2)
    assert r.status_code == 200, r.text
    shard1 = r.json()["shard_id"]

    # 3) 把“浓度”改名为“硝酸盐”（身份不变，字段版 3）
    r = client.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"})
    assert r.status_code == 200, r.text
    assert r.json()["new_version"] == 3
    assert r.json()["indicator_id"] == nitrate_id  # 身份不变

    # 4) 再新增指标“浓度”（同名复用 → 全新身份，字段版 4），语义为磷酸盐
    r = client.post("/schema/indicators", json={"name": "浓度"})
    assert r.status_code == 201, r.text
    v4 = r.json()["new_version"]
    phosphate_id = r.json()["indicator_id"]
    assert phosphate_id != nitrate_id  # 新身份，不继承旧值

    # 5) 用旧字段版 2 再传一份：旧版“浓度”仍存续，必须按原身份（硝酸盐）接入
    f2 = make_parquet("oldver.parquet", {
        "station": ["B"],
        "sampled_at": ["2026-01-02T00:00:00Z"],
        "浓度": [22.0],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, f2, v2)
    assert r.status_code == 200, r.text
    assert r.json()["stored_columns"] == [f"i{nitrate_id}"]

    # 6) 用当前字段版 4 上传：当前“浓度”是磷酸盐新身份
    f3 = make_parquet("newver.parquet", {
        "station": ["C"],
        "sampled_at": ["2026-01-03T00:00:00Z"],
        "浓度": [33.25],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, f3, v4)
    assert r.status_code == 200, r.text
    assert r.json()["stored_columns"] == [f"i{phosphate_id}"]

    # 7) 首次查询，获得快照号；按当前名合读三个分片
    r = client.get("/rows")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["snapshot_created"] is True
    assert body["indicator_names"] == ["硝酸盐", "浓度"]  # 按身份 ID 排序
    assert body["total_rows"] == 3

    by_station = {row["station"]: row for row in body["rows"]}
    # 历史行：硝酸盐 11.5；磷酸盐列在旧片为 NULL
    assert by_station["A"]["硝酸盐"] == 11.5
    assert by_station["A"]["浓度"] is None
    # 旧版上传归到硝酸盐原身份
    assert by_station["B"]["硝酸盐"] == 22.0
    assert by_station["B"]["浓度"] is None
    # 新版上传归到磷酸盐新身份；硝酸盐列在该片为 NULL
    assert by_station["C"]["浓度"] == 33.25
    assert by_station["C"]["硝酸盐"] is None

    # 8) 物理校验：分片 1/2 只有 i{nitrate}，分片 3 只有 i{phosphate} —— 不串列
    t1 = pq.read_table(str(DATA_DIR / "shards" / f"shard-{shard1:010d}.parquet"))
    assert f"i{phosphate_id}" not in t1.column_names
    assert f"i{nitrate_id}" in t1.column_names


def test_old_version_upload_after_rename_keeps_identity(client, make_parquet):
    """改名后用旧版上传，列仍归旧身份；用新版上传旧名应报未知列。"""
    v = client.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    f = make_parquet("a.parquet", {
        "station": ["A"], "sampled_at": ["2026-02-01T00:00:00Z"], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, v).status_code == 200

    client.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"})
    schema = client.get("/schema").json()
    current_version = schema["current_version"]

    # 旧版仍可按旧名接入原身份
    f2 = make_parquet("b.parquet", {
        "station": ["B"], "sampled_at": ["2026-02-02T00:00:00Z"], "浓度": [2.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f2, v).status_code == 200

    # 新版里已经没有“浓度”这个名字 -> 整次拒绝
    r = upload_file(client, f2, current_version)
    assert r.status_code == 409
    assert "不存在" in r.json()["detail"]


# ---------------------------------------------------------------------------
# 删除：含已删指标整次拒绝；删除后同名复用不继承
# ---------------------------------------------------------------------------

def test_deleted_indicator_upload_rejected_and_no_partial(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "氨氮"}).json()["new_version"]
    f = make_parquet("ok.parquet", {
        "station": ["A"], "sampled_at": ["2026-03-01T00:00:00Z"], "氨氮": [5.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, v).status_code == 200
    shards_before = client.get("/rows").json()["total_rows"]

    r = client.delete("/schema/indicators/氨氮")
    assert r.status_code == 200
    delete_version = r.json()["new_version"]

    # 用旧版（含已删指标）上传 -> 整次拒绝
    r = upload_file(client, f, v)
    assert r.status_code == 409
    assert "已被删除" in r.json()["detail"]

    # 用删除后的新版上传同名列 -> 该版已无此列，未知列拒绝
    r = upload_file(client, f, delete_version)
    assert r.status_code == 409

    # 失败后没有任何分片/行数变化，且无临时文件残留
    assert client.get("/rows").json()["total_rows"] == shards_before
    leftovers = list((DATA_DIR / "shards").glob(".tmp-*.parquet"))
    assert leftovers == []

    # 同名复用：新身份，旧分片该指标为 NULL
    r = client.post("/schema/indicators", json={"name": "氨氮"})
    assert r.status_code == 201
    assert r.json()["indicator_id"] >= 2  # 新身份（原身份为 1），关键是旧值不继承
    body = client.get("/rows").json()
    assert body["total_rows"] == 1
    assert body["rows"][0]["氨氮"] is None  # 旧身份值不会出现在新身份名下


# ---------------------------------------------------------------------------
# 快照：分页、字段变更、上传、重启都不改变快照
# ---------------------------------------------------------------------------

def test_snapshot_is_immutable_across_changes_and_restart(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "x"}).json()["new_version"]
    f = make_parquet("s.parquet", {
        "station": ["A", "B"],
        "sampled_at": ["2026-04-01T00:00:00Z", "2026-04-02T00:00:00Z"],
        "x": [1.0, 2.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, v).status_code == 200

    first = client.get("/rows", params={"page_size": 1}).json()
    snap = first["snapshot_id"]
    assert first["total_rows"] == 2
    assert first["total_pages"] == 2
    assert len(first["rows"]) == 1
    page2 = client.get("/rows", params={"snapshot_id": snap, "page": 2, "page_size": 1})
    assert page2.json()["rows"][0]["station"] == "B"

    # 快照之后：改名 + 新增 + 再上传，均不影响快照
    client.post("/schema/indicators/rename", json={"old_name": "x", "new_name": "y"})
    client.post("/schema/indicators", json={"name": "z"})
    f2 = make_parquet("s2.parquet", {
        "station": ["C"], "sampled_at": ["2026-04-03T00:00:00Z"],
        "y": [9.0], "z": [8.0],
    }, types={"sampled_at": pa.string()})
    cur = client.get("/schema").json()["current_version"]
    assert upload_file(client, f2, cur).status_code == 200

    snap_view = client.get("/rows", params={"snapshot_id": snap, "page_size": 100}).json()
    assert snap_view["indicator_names"] == ["x"]          # 字段集合冻结
    assert snap_view["total_rows"] == 2                   # 分片集合冻结
    assert {r["station"] for r in snap_view["rows"]} == {"A", "B"}

    # 元数据接口同样冻结
    meta = client.get(f"/snapshots/{snap}").json()
    assert meta["indicator_names"] == ["x"]
    assert meta["shard_ids"] == [1]

    # 新查询（不带快照号）看到当前状态
    fresh = client.get("/rows").json()
    assert fresh["snapshot_id"] != snap
    assert fresh["indicator_names"] == ["y", "z"]
    assert fresh["total_rows"] == 3

    # 重启：重新 init 后旧快照依然可查（持久化在 SQLite + Parquet）
    del client
    init_db()
    with TestClient(app) as client2:
        r = client2.get("/rows", params={"snapshot_id": snap, "page_size": 100})
        assert r.status_code == 200
        revived = r.json()
        assert revived["indicator_names"] == ["x"]
        assert revived["total_rows"] == 2
        assert {row["x"] for row in revived["rows"]} == {1.0, 2.0}


# ---------------------------------------------------------------------------
# 校验：固定字段、未知列、空文件、类型、上限、保留名
# ---------------------------------------------------------------------------

def test_validation_rules(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "m"}).json()["new_version"]

    # 缺固定字段
    bad = make_parquet("no_station.parquet", {
        "sampled_at": ["2026-05-01T00:00:00Z"], "m": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, bad, v).status_code == 409

    # 未知列
    bad2 = make_parquet("unknown.parquet", {
        "station": ["A"], "sampled_at": ["2026-05-01T00:00:00Z"],
        "m": [1.0], "幽灵": [2.0],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, bad2, v)
    assert r.status_code == 409 and "不存在" in r.json()["detail"]

    # 空 station
    bad3 = make_parquet("null_station.parquet", {
        "station": [None], "sampled_at": ["2026-05-01T00:00:00Z"], "m": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, bad3, v).status_code == 409

    # 布尔指标列拒绝
    bad4 = make_parquet("bool.parquet", {
        "station": ["A"], "sampled_at": ["2026-05-01T00:00:00Z"], "m": [True],
    }, types={"sampled_at": pa.string(), "m": pa.bool_()})
    assert upload_file(client, bad4, v).status_code == 409

    # 空 parquet（0 行）拒绝
    empty = make_parquet("empty.parquet", {
        "station": pa.array([], pa.string()),
        "sampled_at": pa.array([], pa.string()),
        "m": pa.array([], pa.float64()),
    })
    assert upload_file(client, empty, v).status_code == 409

    # 不存在的字段版 -> 404
    good = make_parquet("good.parquet", {
        "station": ["A"], "sampled_at": ["2026-05-01T00:00:00Z"], "m": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, good, 999).status_code == 404

    # 全 NULL 指标列允许（可空浮点）
    nulls = make_parquet("nulls.parquet", {
        "station": ["A"], "sampled_at": ["2026-05-02T00:00:00Z"], "m": [None],
    }, types={"sampled_at": pa.string(), "m": pa.float64()})
    assert upload_file(client, nulls, v).status_code == 200

    # 非 parquet 内容
    r = client.post("/ingest/parquet", data={"schema_version": str(v)},
                    files={"file": ("x.parquet", b"not a parquet", "application/octet-stream")})
    assert r.status_code == 409

    # 保留名 / 重名 / 上限
    assert client.post("/schema/indicators", json={"name": "station"}).status_code == 409
    assert client.post("/schema/indicators", json={"name": "m"}).status_code == 409
    for i in range(7):  # 已有 m，再建 7 个到上限 8
        assert client.post("/schema/indicators", json={"name": f"n{i}"}).status_code == 201
    assert client.post("/schema/indicators", json={"name": "overflow"}).status_code == 409


# ---------------------------------------------------------------------------
# 并发：字段变更与上传交错，全部按已提交字段状态裁决，不出现串列
# ---------------------------------------------------------------------------

def test_concurrent_rename_and_upload(client, make_parquet):
    v = client.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    nitrate = client.get("/schema").json()["indicators"][0]["indicator_id"]

    barrier = threading.Barrier(4)
    errors: list[Exception] = []

    def rename_loop():
        try:
            barrier.wait()
            for _ in range(5):
                client.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"})
                client.post("/schema/indicators/rename", json={"old_name": "硝酸盐", "new_name": "浓度"})
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def upload_loop_old_version(suffix):
        try:
            barrier.wait()
            for i in range(5):
                f = make_parquet(f"old-{suffix}-{i}.parquet", {
                    "station": [f"O{suffix}{i}"],
                    "sampled_at": [f"2026-06-0{suffix+1}T00:00:0{i}Z"],
                    "浓度": [float(i)],
                }, types={"sampled_at": pa.string()})
                r = upload_file(client, f, v)  # 固定旧版：浓度永远映射原身份
                assert r.status_code == 200, r.text
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=rename_loop)]
    threads += [threading.Thread(target=upload_loop_old_version, args=(s,)) for s in (1, 2, 3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    # 最终名可能停在“浓度”或“硝酸盐”，但所有旧版上传的值必须只在原身份列
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT si.indicator_id, COUNT(*) AS n FROM shard_indicators si GROUP BY si.indicator_id"
        ).fetchall()
    finally:
        conn.close()
    ids = {r["indicator_id"] for r in rows}
    assert ids == {nitrate}  # 没有任何值被写到别的身份

    body = client.get("/rows", params={"page_size": 10000}).json()
    current_name = body["indicator_names"][0]
    assert len(body["rows"]) == 15
    assert all(row[current_name] is not None for row in body["rows"])


def test_docs_and_openapi_available(client):
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json").json()
    assert "/ingest/parquet" in spec["paths"]
    assert "/rows" in spec["paths"]


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def upload_file(client, path, version: int):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (path.name, fh.read(), "application/vnd.apache.parquet")},
        )
