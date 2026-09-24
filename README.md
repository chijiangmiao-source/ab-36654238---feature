# calibsvc — 晶圆量测设备标定参数服务

按设备维护随批次区间（半开区间 `[lower, upper)`）变化的标定参数。每次发布生成一个
递增修订号，新区间只替换上一修订的重叠部分，保留左右残段并合并内容相同的相邻段；
所有历史修订不可变、可随时复算。

## 快速开始

```bash
# 构建并启动 db + app + verify（verify 跑完构建检查、单元测试和 HTTP 冒烟后退出）
docker compose up --build --exit-code-from verify verify

# 只启动服务（宿主机端口可用 HOST_PORT 配置，默认 8080）
HOST_PORT=9090 docker compose up --build app
```

`verify` 服务只在 `app` 健康（数据库可达、迁移完成）后启动，执行一次后按结果退出：
全部通过退出码 0，任何一步失败退出码 1。

## API

### 健康检查

```
GET /health  → 200 {"status":"ok"}   # 数据库不可达时 503
```

### 发布标定

```
POST /v1/equipments/{equipment}/calibrations
{
  "operation_id":  "op-2026-0001",   // 操作标识（幂等键，设备内唯一）
  "seen_revision": 3,                // 调用方所见修订号（乐观并发控制）
  "lower": 50, "upper": 150,         // 半开批次区间 [lower, upper)
  "content": { "recipe": "R1", "gain": 1.5 }   // 任意 JSON 标定内容
}

→ 201
{
  "equipment": "EQ-1",
  "revision": 4,                     // 新的递增修订号
  "segments": [                      // 该修订生效的完整区间快照
    {"lower": 0,   "upper": 50,  "content": {...}},
    {"lower": 50,  "upper": 150, "content": {...}},
    {"lower": 150, "upper": 300, "content": {...}}
  ]
}
```

- 每次成功发布修订号 +1，新快照替换重叠部分、保留残段、合并内容相同的相邻段。
- 同一 `operation_id` + 相同参数重试：返回首次结果（201 + 首次响应体，
  响应头 `X-Idempotent-Replay: true`），不产生新修订。
- 同一 `operation_id` + 不同参数：`409 OPERATION_CONFLICT`。
- `seen_revision` 与当前修订不一致：`409 STALE_REVISION`（并发发布同一所见
  修订时只有一个能成功）。
- 内容按规范化 JSON 比较（键序、空白不敏感），用于相邻段合并与幂等哈希。

### 查询生效标定

```
GET /v1/equipments/{equipment}/calibration?batch=75[&revision=2]

→ 200
{
  "equipment": "EQ-1",
  "batch": 75,
  "revision": 2,        // 实际使用的修订（省略时为当前最新）
  "lower": 50, "upper": 150,
  "content": {...}
}
```

`revision` 省略时查当前最新修订；指定时复算该历史修订当时唯一生效的内容、
区间边界与修订号。历史修订只增不改，任何时刻都可复算。

### 错误码（稳定）

| HTTP | code                  | 含义                                   |
|------|-----------------------|----------------------------------------|
| 400  | `INVALID_JSON`        | 请求体不是合法 JSON / 含未知字段       |
| 400  | `INVALID_PARAMETER`   | 缺少必填参数或参数非法                 |
| 400  | `INVALID_INTERVAL`    | 区间非法（`lower >= upper`）           |
| 400  | `INVALID_CONTENT`     | content 不是合法 JSON                  |
| 404  | `EQUIPMENT_NOT_FOUND` | 设备不存在                             |
| 404  | `REVISION_NOT_FOUND`  | 修订号不存在（超出当前修订）           |
| 404  | `BATCH_NOT_COVERED`   | 批次未被任何区间覆盖                   |
| 409  | `STALE_REVISION`      | 所见修订已过期（并发冲突）             |
| 409  | `OPERATION_CONFLICT`  | 操作标识被不同参数复用                 |
| 503  | `UNHEALTHY`           | 健康检查时数据库不可达                 |

## 设计

**数据模型**（PostgreSQL，`internal/store`）

- `heads(equipment, revision)`：每台设备的当前修订号。
- `segments(equipment, revision, lower, upper, content)`：每个修订的完整区间
  快照，只插不改不删——数据库触发器拒绝任何 `UPDATE`/`DELETE`，历史修订永远
  可复算。
- `operations(equipment, operation_id, request_hash, revision, response)`：
  幂等账本，记录首次响应，同样受不可变触发器保护。

**发布事务**（单事务，失败即整体回滚，不留部分切分）

1. `INSERT ... ON CONFLICT DO NOTHING` 建 head 行，随后 `SELECT ... FOR UPDATE`
   锁定——同一设备的发布由此串行化。
2. 先查幂等账本：同操作同参数 → 直接重放首次结果；同操作异参数 →
   `OPERATION_CONFLICT`。（先于修订检查，保证过期修订的合法重试仍返回首次结果。）
3. 校验 `seen_revision == head`，否则 `STALE_REVISION`。
4. 读取当前修订区间，用 `internal/split.Apply` 计算新快照（替换重叠、保留
   残段、合并同内容相邻段），整批写入新修订并推进 head，记录幂等账本，提交。

**区间切分**（`internal/split`，纯函数，表驱动单元测试覆盖）

新区间 `[L,U)` 作用于有序不重叠段集：重叠段留下 `[s.L,L)` 与 `[U,s.U)` 残段，
中段被新段替换，最后对相邻同内容段做合并；输出保持有序、不重叠、无退化段。

## 验证（compose 中的 verify 服务）

`verify` 一次性运行并据结果退出：

- 构建检查：`go build ./...`、`go vet ./...`
- 单元测试：`go test ./...`（区间切分、JSON 规范化）
- HTTP 冒烟（对运行中的服务）：
  - 区间切分与覆盖：中间批次补发、残段保留、同内容合并、未覆盖批次报错
  - 历史不变：旧修订查询结果不随后续发布改变（API 层 + 数据库触发器层）
  - 幂等重放：同操作同内容（含并发）返回首次结果，异参复用报 `OPERATION_CONFLICT`
  - 并发过期冲突：8 个并发发布同一所见修订，恰好 1 个成功、其余
    `STALE_REVISION`，head 恰好前进 1
  - 参数校验与稳定错误码；失败事务不留任何状态

## 配置

| 变量           | 作用                         | 默认        |
|----------------|------------------------------|-------------|
| `DATABASE_URL` | PostgreSQL DSN（app 必填）   | —           |
| `PORT`         | app 容器内监听端口           | `8080`      |
| `HOST_PORT`    | 宿主机映射端口（compose）    | `8080`      |
| `APP_URL`      | verify 访问的服务地址        | 见 compose  |

## 项目结构

```
cmd/server/        服务入口（健康路径、优雅退出）
cmd/verify/        一次性验收：构建检查 + 单元测试 + HTTP 冒烟
internal/split/    区间切分纯函数（单元测试）
internal/canonjson/ JSON 规范化（内容相等性）
internal/store/    PostgreSQL 持久化与发布事务
internal/httpapi/  HTTP 路由、参数校验、错误映射
Dockerfile         runtime（服务）与 verify（验收）两个构建目标
docker-compose.yml db + app + verify 编排，健康依赖与可配置端口
```
