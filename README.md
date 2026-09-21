# batchseal — 批次分片乱序接收与封存服务（Go 1.23 纯后端 API）

测序仪网络抖动会导致批次分片**乱序、重传、甚至同序号内容冲突**。本服务在只有收到
**全部**声明分片后，才允许批次原子进入 `SEALED` 终态；所有裁决落在 PostgreSQL
行锁上，因此两个（或更多）无状态 API 实例共享同一数据库时结论永远一致，并可在
全部实例重启后保持确认、缺口与封存结果不变。

- 语言/运行时：Go 1.23，仅标准库 + [`pgx/v5`](https://github.com/jackc/pgx)，纯 HTTP/JSON
- 存储：PostgreSQL（批次表、分片表、终态）
- 部署：Docker Compose，两个 API 实例 `api1`/`api2` + 一个一次性验收服务 `verify`
- 只有 `api1` 向宿主机发布端口，宿主端口可用 `API_PORT` 覆盖（默认 `8080`）

---

## 快速开始

```bash
# 启动 db / api1 / api2（仅 api1 发布宿主端口 8080）
docker compose up -d --build db api1 api2

# 一次性验收（经内部网络访问两个实例）
docker compose run --rm verify

# 完整验收：验收 -> 重启全部 API -> 再验收（确认重启一致性）
./scripts/acceptance.sh
# 或
make acceptance
```

用其它宿主端口发布：

```bash
API_PORT=18080 docker compose up -d --build
```

停止并清理数据卷：

```bash
docker compose down -v
```

---

## API

所有请求/响应均为 `application/json; charset=utf-8`。时间为 UTC RFC3339。

### 1. 创建批次

`POST /api/v1/batches`

请求：

```json
{ "expectedChunks": 3 }
```

`expectedChunks` 为整数，范围 **1–10000**（含端点）。可选布尔字段
`protectWrites`（省略或 `false` 沿用无鉴权旧协议；`true` 开启写保护，见第 5 节）。

`201 Created`（未保护）：

```json
{
  "batchId": "182a8a5485765bafb286a7a8e4ce7c69",
  "expectedChunks": 3,
  "status": "OPEN",
  "createdAt": "2026-09-18T19:57:38.245878Z"
}
```

`201 Created`（`"protectWrites": true`，**仅此一次**额外返回 `writeToken`）：

```json
{
  "batchId": "182a8a5485765bafb286a7a8e4ce7c69",
  "expectedChunks": 3,
  "status": "OPEN",
  "createdAt": "2026-09-18T19:57:38.245878Z",
  "writeToken": "zC0dQ9vS2mYp7bN1kW8xL4fH6tR3jG5aV0oU9eP2nQs"
}
```

### 2. 提交分片（允许乱序 / 重传 / 冲突）

`POST /api/v1/batches/{batchId}/chunks`

```json
{ "seq": 1, "payload": "café" }
```

- `seq`：整数，**1 ≤ seq ≤ expectedChunks**
- `payload`：字符串，UTF-8 编码后**至多 65536 字节**（按 UTF-8 字节判同，不是字符数）

| 情况 | 状态码 | 说明 |
|---|---|---|
| 首次写入 | **201** | `duplicate:false`，返回确认 |
| 同序号、**字节完全相同**的重传 | **200** | `duplicate:true`，返回**原确认**（`receivedAt` 不变），不新增记录 |
| 同序号、内容不同 | **409** | `error:"CHUNK_CONFLICT"` |
| 封存后改内容 / 新内容 | **409** | `error:"BATCH_SEALED"` |
| 封存后同内容重传 | **200** | 仍返回原确认 |

确认响应：

```json
{
  "batchId": "182a8a5485765bafb286a7a8e4ce7c69",
  "seq": 1,
  "size": 5,
  "duplicate": false,
  "receivedAt": "2026-09-18T19:57:52.390466Z"
}
```

冲突响应体：

```json
{ "error": "CHUNK_CONFLICT", "message": "chunk seq already exists with a different payload" }
```

### 3. 查询批次状态

`GET /api/v1/batches/{batchId}` → `200`

```json
{
  "batchId": "182a8a5485765bafb286a7a8e4ce7c69",
  "status": "OPEN",
  "expected": 3,
  "received": 1,
  "gaps": [2, 3]
}
```

`gaps` 为**升序**缺失序号；无缺口时为 `[]`。封存后额外包含 `sealedAt`。

### 4. 封存

`POST /api/v1/batches/{batchId}/seal`

- 序号全集齐备：**原子**进入 `SEALED`，返回 `200` 与终态
- 仍有缺口：返回 **409 `INCOMPLETE`**（含升序 `gaps`），批次**保持 `OPEN`**
- 对已 `SEALED` 批次重复封存：返回 **200** 与**既有封存结果**（`sealedAt` 不变）

`409 INCOMPLETE` 示例：

```json
{
  "error": "INCOMPLETE",
  "message": "batch is missing chunks",
  "batchId": "...",
  "status": "OPEN",
  "expected": 5,
  "received": 3,
  "gaps": [2, 4]
}
```

### 5. 写保护（`protectWrites` / `X-Batch-Write-Token`）

创建批次时传 `"protectWrites": true`，服务在 **201 响应中且仅这一次** 返回
`writeToken`。它是 256 位随机值的**无填充 base64url**（43 字符）；数据库只保存
其 **SHA-256 摘要**（`write_token_digest`，32 字节），从不保存明文令牌，状态查询
也绝不暴露令牌或摘要。请妥善保存该令牌——丢失即无法再写入。

受保护批次的以下操作都要带请求头 `X-Batch-Write-Token: <token>`：

- 提交分片 `POST …/chunks`
- 封存 `POST …/seal`
- 轮换 `POST …/write-token/rotate`

鉴权**不在 HTTP 层做预校验**：每个请求都在事务内先 `SELECT … FOR UPDATE` 取
`batches` 行锁，然后在该锁内用**恒定时间**比较摘要，再于同一把锁内完成原本的
裁决（序号、冲突、缺口、封存）或轮换。因此在轮换提交前排队的请求仍按旧令牌裁决，
轮换提交后排队的请求会看到新摘要而失败。

- 令牌缺失或错误：在检查序号、冲突、缺口、封存状态**之前**统一返回
  **403 `WRITE_CAPABILITY_REQUIRED`**，且**不改动任何数据**。开放、缺片、已封存
  三种状态下的 403 响应完全相同，无法据此探测批次状态。
- 未保护批次（省略/`false`，或功能上线前的旧行）：分片与封存完全沿用旧协议，
  任何令牌头都被忽略；对其调用轮换返回 **409 `BATCH_NOT_PROTECTED`**。
- 轮换：持当前有效令牌调用 `POST …/write-token/rotate`，成功以 **200** 一次性返回
  新 `writeToken`，并在同一行锁内**原子替换**摘要；未提交时旧令牌仍有效，一旦
  提交旧令牌立即失效，仅新令牌可写。

轮换成功响应：

```json
{ "batchId": "182a8a5485765bafb286a7a8e4ce7c69", "writeToken": "…新令牌…" }
```

> 旧库启动时会幂等地 `ALTER TABLE batches ADD COLUMN IF NOT EXISTS
> write_token_digest BYTEA`；既有行该列为 `NULL`，按未保护处理。

---

## 固定状态码表

| 场景 | 状态码 | `error` 代码 |
|---|---|---|
| 创建批次成功 | 201 | — |
| `expectedChunks` 不在 1–10000 | 400 | `INVALID_EXPECTED_CHUNKS` |
| 请求体非合法 JSON / 缺字段 | 400 | `INVALID_REQUEST` |
| `seq` 非整数或 `< 1` | 400 | `INVALID_SEQ` |
| `seq` 超过 `expectedChunks` | 400 | `SEQ_OUT_OF_RANGE` |
| `payload` 非字符串 | 400 | `INVALID_PAYLOAD` |
| `payload` 超过 65536 UTF-8 字节 | 413 | `PAYLOAD_TOO_LARGE` |
| 批次不存在 / ID 非法 | 404 | `BATCH_NOT_FOUND` |
| 同序号不同内容 | 409 | `CHUNK_CONFLICT` |
| 封存后变更内容 | 409 | `BATCH_SEALED` |
| 缺片封存 | 409 | `INCOMPLETE`（携带 `gaps`，保持 OPEN） |
| 受保护批次缺/错令牌（分片、封存、轮换，任何状态） | 403 | `WRITE_CAPABILITY_REQUIRED` |
| 对未保护批次轮换令牌 | 409 | `BATCH_NOT_PROTECTED` |
| 轮换成功 | 200 | —（一次返回新 `writeToken`） |
| 分片首次写入 | 201 | — |
| 相同内容重传 / 封存后相同重传 | 200 | — |
| 齐备封存成功 / 重复封存 | 200 | — |
| 健康检查 `GET /healthz` | 200 | — |

---

## 乱序示例

```bash
B=$(curl -s localhost:8080/api/v1/batches -d '{"expectedChunks":3}' | jq -r .batchId)

curl -s -X POST localhost:8080/api/v1/batches/$B/chunks -d '{"seq":3,"payload":"γ"}'  # 201
curl -s -X POST localhost:8080/api/v1/batches/$B/chunks -d '{"seq":3,"payload":"γ"}'  # 200 duplicate, 原确认
curl -s -X POST localhost:8080/api/v1/batches/$B/chunks -d '{"seq":3,"payload":"δ"}'  # 409 CHUNK_CONFLICT
curl -s -X POST localhost:8080/api/v1/batches/$B/chunks -d '{"seq":1,"payload":"a"}'  # 201
curl -s localhost:8080/api/v1/batches/$B                                              # gaps:[2]
curl -s -X POST localhost:8080/api/v1/batches/$B/seal                                 # 409 INCOMPLETE gaps:[2]
curl -s -X POST localhost:8080/api/v1/batches/$B/chunks -d '{"seq":2,"payload":"b"}'  # 201
curl -s -X POST localhost:8080/api/v1/batches/$B/seal                                 # 200 SEALED
curl -s -X POST localhost:8080/api/v1/batches/$B/seal                                 # 200 既有结果
```

两个实例返回一致：`api1` 写入，可立即从 `api2`（仅内部网络可达）读到同一终态。

---

## 并发正确性：为什么不会“已封存却缺片”

提交分片与封存都在事务里先对 `batches` 行取 `SELECT … FOR UPDATE`：

- **封存事务**锁住批次行后统计分片数与缺口，齐备才 `UPDATE … SET status='SEALED'`；
  有缺口则返回 `INCOMPLETE` 且不改动状态。
- **最后一片的提交事务**同样锁该行后再插入。

因此“最后一片”和“封存”在两个进程上同时发生时，PostgreSQL 强制二者串行：

- 封存先拿锁 → 看到缺口 → `INCOMPLETE`，随后分片落库，批次保持 OPEN（再封存即成功）；
- 分片先拿锁 → 集齐，随后封存看到完整集合 → `SEALED`。

任何快照下都不可能出现 `status=SEALED` 而 `gaps` 非空。同序号两个不同内容并发时，
一个事务先插入并提交，另一个读到已存字节并返回 `CHUNK_CONFLICT`，分片表始终只有一行。

状态接口在一条 SQL 快照中同时计算 `received` 与 `gaps`，二者永远自洽。

### 写令牌裁决与轮换为什么是原子的

分片提交、封存、轮换都在事务里先对 `batches` 行取 `SELECT … FOR UPDATE`，随后在
同一把行锁内恒定时间比对 `write_token_digest`，再做原裁决或轮换本身。没有任何
HTTP 层预鉴权，所以令牌有效性始终与数据裁决在同一个临界区里判定：

- **轮换先持锁**：旧令牌提交在锁外排队；轮换提交新摘要后，排队的提交被放行，
  读到的是新摘要 → `403 WRITE_CAPABILITY_REQUIRED`，且不写入。
- **提交先持锁**：旧令牌提交先成功落库，排在其后的轮换被放行时仍匹配旧摘要 →
  轮换成功，但从这一刻起旧令牌失效，只有新令牌能写。
- 两个实例并发轮换时，行锁强制串行：只有一个轮换返回 200，另一个随后看到已被
  替换的摘要并返回 403；数据库内任一时刻只有一个有效摘要。

旧行（`write_token_digest IS NULL`）跳过比对，等同未保护，故旧协议行为完全不变。

## 重启一致性

API 实例无状态，全部状态都在 PostgreSQL。重启全部 API 后：

- 已收分片的确认时间不变（重传仍是 `200` + 原 `receivedAt`）；
- 缺口列表、计数不变；
- `SEALED` 批次终态与 `sealedAt` 不变，重复封存继续返回既有结果。

`./scripts/acceptance.sh` 会真实执行“验收 → 重启 api1/api2 → 再验收”。

---

## 本地开发 / 测试

测试使用**真实 PostgreSQL**复现交错与重启（不用 mock、不用内存替身）：

```bash
# 指向任意 Postgres 15+
export TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5432/batchseal?sslmode=disable'
go test -race -count=1 ./...
```

数据库不可达时集成测试自动 `t.Skip`，不会假绿。测试为每个用例建立独立
`search_path` schema 并在用例结束删除。

关键测试：

- `internal/store`：乱序、重传幂等、UTF-8 字节判同、冲突、封存不完整保持 OPEN、
  封存/最后一片跨连接池竞争（30 轮）、双写冲突竞争（30 轮）、关闭全部连接后重开的重启一致性；
  写保护：令牌签发与只存 SHA-256 摘要、开放/缺片/已封存统一 403 且不改数据、
  旧协议兼容、轮换原子替换、**强制交错**的“轮换先持锁→旧提交排队→提交后 403 无写入”
  与“旧提交先成功→随后失效”、轮换/写入并发（20 轮）、旧 schema 升级与旧行未保护；
- `internal/api`：两个独立连接池支撑的两个 HTTP 实例上交替写入/封存、全状态码矩阵、
  跨实例封存竞争（20 轮）、关闭并重建两个实例后的终态一致性；
  写保护：导线协议矩阵、三种状态一致 403、轮换矩阵，以及用真实 PostgreSQL 行锁
  **强制两种交错顺序**的双实例轮换/旧提交竞态（请求真实穿过 HTTP 实例）、20 轮并发轮换；
- `cmd/verify`：Compose 内一次性验收程序，协议矩阵 + 25 轮跨实例封存竞争 +
  25 轮跨实例轮换/写入竞争 + 受保护批次重启两阶段。

环境变量：

| 变量 | 默认 | 用途 |
|---|---|---|
| `API_PORT` | `8080` | API 监听端口（容器内）；宿主发布端口同变量覆盖 |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/batchseal?sslmode=disable` | 数据库连接串 |
| `TEST_DATABASE_URL` | 同上 | Go 集成测试使用 |
| `VERIFY_STATE_DIR` | `/verify-state` | verify 两阶段间持久化夹具 ID/令牌的目录（本地运行可覆盖） |

---

## 目录结构

```
cmd/
  api/main.go          # API 进程
  verify/main.go       # 一次性验收服务（compose 服务 verify）
internal/
  api/                 # HTTP 路由、状态码、请求校验 + HTTP 集成测试
  store/               # PostgreSQL 存储、行锁事务、schema 嵌入 + 存储测试
  testsupport/         # 测试用 schema/连接辅助
scripts/acceptance.sh  # 含全部 API 重启的端到端验收
Dockerfile             # 多阶段构建 api 与 verify（distroless 静态镜像）
docker-compose.yml     # db + api1（唯一发布端口）+ api2（仅内网）+ verify
```
