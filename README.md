# 水质数仓化验表 API

固定站点、UTC 采样时刻，外加至多 8 个可空浮点指标的化验表服务。
技术栈：**FastAPI + PyArrow + SQLite + Parquet 分片**，可通过 Docker Compose 一键启动。

## 1. 核心模型（为什么改名/删名不会串列）

| 概念 | 说明 |
| --- | --- |
| 指标身份 `indicator_id` | 指标的稳定身份。**改名只改名字，不换身份**；删除只是 `active=0`，身份永不复用。 |
| 字段版 `schema_version` | 每次新增/改名/删除都产生一个不可变新版本，版内固化“字段名 → 指标身份”的完整映射。 |
| 分片 shard | 每次 Parquet 上传产生一个分片，物理列名为身份列 `i{indicator_id}`，与当前叫什么名字完全解耦。 |
| 快照 snapshot | 首次查询时固化“当时的字段名集合 + 当时的全部分片”，此后分页、字段变更、上传、重启都不改变它。 |

典型故事（与需求一致）：

1. 建指标 **浓度**（身份 1，字段版 2），上传历史分片 → 物理列 `i1`。
2. **浓度 改名为 硝酸盐**（身份仍是 1，字段版 3）。
3. 再建指标 **浓度**（同名复用 → 身份 2，字段版 4，语义为磷酸盐，不继承任何旧值）。
4. 用**旧字段版 2** 上传：旧版“浓度”映射到身份 1，数据进 `i1`（硝酸盐），不串列。
5. 用**字段版 4** 上传：当前“浓度”映射到身份 2，数据进 `i2`（磷酸盐）。
6. 查询按当前名合读：旧分片没有 `i2` → 磷酸盐列为 `NULL`；新分片没有 `i1` → 硝酸盐列为 `NULL`。

## 2. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务：http://localhost:8000
# 交互式文档（Swagger）：http://localhost:8000/docs
# OpenAPI：http://localhost:8000/openapi.json
# 健康检查：http://localhost:8000/health
```

数据持久化在命名卷 `water_quality_data`（容器内 `/data`，含 `warehouse.db` 与 `shards/*.parquet`），
重启容器后字段、分片、快照全部保留。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 3. API 一览

| 方法/路径 | 作用 |
| --- | --- |
| `GET /schema` | 当前字段版号、固定字段、存续指标（含身份 ID） |
| `GET /schema/versions/{version_id}` | 查看任意字段版的不可变映射（含已删指标） |
| `POST /schema/indicators` | 新增指标（body `{"name": "..."}`）；同名复用得到**新身份** |
| `POST /schema/indicators/rename` | 改名（body `{"old_name": "...", "new_name": "..."}`），身份不变 |
| `DELETE /schema/indicators/{name}` | 删除指标（历史分片保留） |
| `POST /ingest/parquet` | 表单上传：`schema_version`（必填）+ `file`（Parquet） |
| `GET /rows` | 分页查询；首次不带 `snapshot_id` 即建快照，之后带快照号翻页（原始值读取，不受发布影响） |
| `GET /snapshots/{snapshot_id}` | 快照元数据（固化的字段名与分片） |
| `POST /verification/publish` | **核对发布**：提交快照号/站点/UTC 时刻/指标 ID + 1~20 个对照值（各带绝对容差），全配上才整组发布 |
| `GET /analysis/published` | **分析读取**：只列已发布记录（可按快照/站点/指标过滤） |
| `GET /analysis/published/{publication_id}` | 读取单条已发布记录的配对明细与双方来源 |
| `GET /docs` | Swagger UI |

## 3A. 核对发布与分析读取

### 业务语义

水质交接员向分析组移交某站同一时刻的平行样前，需要确认每条实测都有对应的
交接值。核对发布接口在**指定快照**内完成裁决：

1. 请求体：`snapshot_id`（已有快照号）、`station`、`sampled_at`（UTC 时刻，
   带偏移量自动换算 UTC）、`indicator_id`（指标**身份**，不是名字）、
   `references`（1~20 个 `{value, tolerance, source?}`，各自带绝对容差）。
2. 只取该快照**固化分片**内同键（同站点 + 同 UTC 时刻 + 同指标身份）的实测；
   快照之后的新上传、同名复用得到的新指标身份都不会混入。
3. 指标值为 **NULL 的同键实测不可配对**，单独在失败结果中回报；非 NULL 实测
   **超过 20 条直接报错**（409）。
4. 一对一配对，目标按字典序：**先使容差内配对数最多，再使绝对差总和最小**
   （匈牙利算法，双方各不超过 20）。
5. **只有两侧全部配上**（无 NULL、无多余、无缺失且全部在容差内）才把这组实测
   整组发布；否则返回 409，结构化给出：
   * `extra_measured`：多余实测及其来源（分片 ID + Parquet 物理行号）；
   * `missing_references`：找不到实测的交接对照值及其来源；
   * `null_measured`：同键但值为 NULL 的实测及来源；
   * `best_pairs`：按上述两目标求出的尽力配对（仅供核对，**不会发布**）。
6. 同一 `(快照, 站点, 时刻, 指标身份)` 只允许发布一次；重复提交返回 409，
   原有发布不可更改。

### 已发布记录的不变量

* 发布记录固化指标名与双方来源（实测侧精确到 `shard_id` + 物理行号，
  对照侧为请求提供的 `source`）；后续**改名、删除、同名新指标、新上传**都
  不改变已发布结果。
* 分析读取入口（`/analysis/*`）**只提供已发布记录**，未发布的原始实测不会
  出现；原始值仍由原 `/rows` 接口读取，两者互不影响。
* 发布与字段变更/上传共用同一把进程写锁 + SQLite `BEGIN IMMEDIATE` 事务，
  整组要么完整可见、要么完全不可见，**不存在半组发布**；记录持久化在
  SQLite，进程重启后仍可读取。

### 请求示例

```bash
curl -X POST http://localhost:8000/verification/publish -H 'Content-Type: application/json' -d '{
  "snapshot_id": 1,
  "station": "S1",
  "sampled_at": "2026-09-01T00:00:00Z",
  "indicator_id": 1,
  "references": [
    {"value": 1.05, "tolerance": 0.1, "source": "交接单-WQ-001"},
    {"value": 2.10, "tolerance": 0.2, "source": "交接单-WQ-002"}
  ]
}'
```

成功返回 `publication_id`、固化指标名、每条配对的双方来源与绝对差总和；
随后 `GET /analysis/published/1` 可随时读回完整明细。

### Parquet 上传规则

* 必含固定列：`station`（非空字符串）、`sampled_at`（UTC 时刻；支持带时区 timestamp、
  ISO-8601 字符串含 `Z`/偏移量、date；无时区值按 UTC 处理）。
* 指标列必须可转 `float64`（整数允许、布尔拒绝、全 NULL 允许），至多 8 个。
* 列名按**指定字段版**裁决：
  * 版内不存在的列（未知列）→ **整次拒绝（409）**；
  * 列映射到已删除指标 → **整次拒绝（409）**；
  * 版内未包含的指标可以缺列（指标本身可空）。
* 空文件、0 行分片、无法解析的 Parquet → 409；字段版不存在 → 404。
* 失败只回滚事务并清理临时文件，**不改动任何已有字段、数据与快照**。

### 查询与快照语义

* `GET /rows?page=1&page_size=100`：首次调用返回 `snapshot_id` 与 `snapshot_created=true`。
* 之后 `GET /rows?snapshot_id=...&page=2&page_size=100` 始终读同一批分片、同一组字段名；
  新字段在旧分片为 `NULL`，同名复用的新身份在旧分片同样为 `NULL`。
* 不带快照号的新查询会生成新快照，反映最新字段与分片。
* 分片内按 `(station, sampled_at)` 稳定排序后分页；`page_size` 上限 10000。

### 并发与一致性

所有字段变更、上传与快照创建都在同一把进程写锁 + SQLite `BEGIN IMMEDIATE` 事务内裁决：
字段变更与上传交错时，每个请求看到的都是一个完整的已提交字段状态，不存在半份结果。

## 4. 目录结构

```
app/
  config.py        # 常量与数据目录（含核对上限 MAX_CHECK_VALUES=20）
  db.py            # SQLite 建表、连接、立即事务（含 publications 发布表）
  models.py        # Pydantic 模型与错误类型
  catalog.py       # 指标身份与字段版（新增/改名/删除）
  ingest.py        # Parquet 校验、按字段版映射身份、原子落盘
  query.py         # 快照创建与分页合读（/rows 原始值）
  matching.py      # 一对一配对：匈牙利算法，先最多容差配对再最小绝对差总和
  verification.py  # 核对发布：同键实测提取、NULL/超量裁决、整组原子发布
  analysis.py      # 分析读取：只读已发布记录及双方来源
  main.py          # FastAPI 路由与并发控制
tests/             # pytest 自测（含配对算法单测、核对发布端到端）+ HTTP 冒烟脚本
Dockerfile
docker-compose.yml
```

## 5. 自测

```bash
pip install -r requirements.txt
pip install pytest httpx
python -m pytest tests/ -v
# 对已运行的服务做独立 HTTP 冒烟：
python tests/manual_smoke.py
```
