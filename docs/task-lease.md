# 本地任务领取与租约存储层（tasklease）

在现有 FastAPI 服务（`GET /` 行为不变）基础上新增的纯本地持久化任务层。
**无外部服务依赖**：存储基于本地 SQLite 文件（默认 `tasklease.db`，可用环境变量
`TASKLEASE_DB` 覆盖），多个并发消费者（线程或进程）可同时工作，并对任务状态
得到一致且可解释的结论。

代码结构：

| 文件 | 职责 |
| --- | --- |
| `tasklease/models.py` | `Task` 模型与 `TaskStatus` 状态枚举 |
| `tasklease/errors.py` | 可区分的拒绝原因 `RejectReason`、入队/释放/续期结果枚举与 `TaskRejected` |
| `tasklease/schema.py` | 建表与旧库平滑升级（`PRAGMA user_version` + 逐列补齐） |
| `tasklease/store.py` | 核心存储：幂等入队、并发领取、租约、重试、乐观版本 |
| `tasklease/api.py` | 挂载在 `/tasks` 下的 HTTP 接口（可选） |
| `tests/` | 并发领取、租约过期、重复入队、非法流转等测试 |

## 1. 状态机

```
                 enqueue(idempotency_key)
                        │
                        ▼
                   ┌─────────┐  acquire()    ┌─────────┐
                   │ PENDING │ ────────────► │ RUNNING │
                   └─────────┘                └─────────┘
                        ▲                      │  │  │
                        │ fail() 未达上限      │  │  │ complete()
                        │ + 退避等待           │  │  ▼
                        └──────────────────────┘  │ ┌───────────┐
                                        release() │ │ SUCCEEDED │
                                        (主动释放)│ └───────────┘
                                                  ▼
                                            ┌─────────┐
                                  fail()    │ FAILED  │ （终态：重试耗尽）
                                  达到上限 ► └─────────┘
```

- **PENDING**：可领取（需满足 `available_at <= now`，即退避已结束）。
- **RUNNING**：已被某个消费者持有租约；租约过期后可被其它消费者重新领取。
- **SUCCEEDED / FAILED**：终态。终态任务不再被领取，也不能再提交结果或重复入队。

### 入队约束（幂等 + 状态机）

- 同一 `idempotency_key` 的任意次提交都收敛到**同一条任务**，数据库对非空键有
  唯一索引，即使并发提交也不会产生重复记录（返回 `CREATED` 或幂等命中 `EXISTING`）。
- 任务**正在执行且租约有效**时重复入队 → 拒绝，原因 `task_executing`。
- 任务已处于**终态**时重复入队 → 拒绝，原因 `task_terminal`。
- 租约已过期的 RUNNING 任务允许重复提交，但仍收敛到同一任务（等待重新领取）。

### 非法状态流转的可区分拒绝原因

| `RejectReason` | 触发场景 |
| --- | --- |
| `not_pending_or_unclaimed` | 任务尚未被领取（PENDING）就提交结果 |
| `lease_held_by_other` | 当前消费者不是租约持有者 |
| `lease_expired` | 租约已过期，持过期租约提交结果/失败 |
| `task_terminal` | 任务已 SUCCEEDED/FAILED，终态不可再变 |
| `version_conflict` | 基于过期版本的写入被拒绝 |
| `task_not_found` | 任务不存在（HTTP 映射为 404，其余为 409） |

## 2. 租约管理

- `acquire(consumer, lease_seconds)` 用单条条件 `UPDATE ... WHERE rowid =
  (SELECT ... LIMIT 1)` 原子地选取并占用任务：候选为“可领取的 PENDING”或
  “租约已到期的 RUNNING”。因此**同一任务任意时刻只有一个消费者持有**，
  并写入 `lease_owner`、`lease_expires_at`，执行权归属明确。
- 租约判定：`lease_expires_at > now` 有效；`<= now` 即过期（到期点本身视为过期）。
- `heartbeat(task_id, consumer, lease_seconds)` 续期，返回三态：
  - `renewed`：持有者续期成功（版本号 +1）；
  - `not_held`：任务不在运行中或调用者不是持有者；
  - `expired`：租约已过期，续期被拒绝。
- `release(task_id, consumer)` 释放，返回三态：
  - `released`：持有者主动释放，任务回到 PENDING 且立即可重新领取；
  - `not_held`：非持有者或任务不在运行中；
  - `already_expired`：租约已过期，不改写状态，等待领取流程重新分配。
- **持过期租约的消费者不能提交结果**：`complete/fail` 都会先校验租约归属与有效期。

## 3. 失败重试与退避

- 每次 `acquire` 都会令 `attempt += 1`。
- `fail()` 时：
  - 若 `attempt < max_attempts`：任务回到 PENDING，按指数退避设置
    `available_at = now + backoff`（默认 `backoff_base * 2**(attempt-1)`，
    也可显式传 `backoff_seconds`）；退避窗口内任务不可被领取。
  - 若 `attempt >= max_attempts`：进入终态 FAILED，永不再被领取。
- `complete()` 进入终态 SUCCEEDED，清除租约。

## 4. 并发约束与防覆盖

- **进程内**：所有会话经一把可重入锁串行化，连接设置了 `busy_timeout`。
- **跨进程**：SQLite（WAL 模式）写事务天然串行化；领取/释放/续期/提交全部使用
  带状态、持有者、版本、租约到期条件的 UPDATE，冲突时 `rowcount=0`，不会静默覆盖。
- **乐观版本控制**：每行有单调递增的 `version`，每次状态变更 +1。
  `complete/fail` 可携带 `expected_version`，与当前版本不符时拒绝
  （`version_conflict`），而不是让过期写入静默生效。
- 所有关键流转写入 `task_events` 审计表（事件、操作者、from/to 状态、版本、原因），
  保证结论“可解释”。

## 5. 旧数据平滑升级

`schema.initialize()` 以 `PRAGMA user_version` 记录结构版本：

- 全新库直接建最新结构；
- 已存在的旧 `tasks` 表（例如只有 `id,status` 两列）通过逐列
  `ALTER TABLE ADD COLUMN` 补齐，缺失列填兼容默认值（空幂等键、`'{}'` 负载、
  `attempt=0, version=0, available_at=0` 等），**升级后旧数据仍可正常读取并
  继续走完生命周期**；
- 幂等唯一索引只约束**非空**幂等键（部分索引），因此历史空键行不会导致升级失败。

## 6. HTTP 接口（与原服务共存）

挂载在 `/tasks` 前缀下，原有 `GET / -> {"Hello":"World"}` 保持不变：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /tasks` | 幂等入队，body：`idempotency_key, payload, max_attempts` |
| `POST /tasks/acquire` | 领取，body：`consumer, lease_seconds?` |
| `POST /tasks/{id}/complete` | 成功提交，body：`consumer, result?, expected_version?` |
| `POST /tasks/{id}/fail` | 失败上报，body：`consumer, error?, backoff_seconds?, expected_version?` |
| `POST /tasks/{id}/heartbeat` | 租约续期 |
| `POST /tasks/{id}/release` | 主动释放 |
| `GET  /tasks/{id}` | 查询任务 |

非法操作返回 `409`（任务不存在为 `404`），响应形如：

```json
{"detail": {"reason": "lease_expired", "message": "任务 ... 的租约已于 ... 过期 ..."}}
```

## 7. 本地验证

```bash
# 安装依赖（uv）
uv sync

# 运行全部单测（日志会打印入队标识、租约信息与状态流转判定依据）
uv run pytest -q
uv run pytest -s -o log_cli=true          # 查看详细判定日志

# 启动 HTTP 服务并手动验证
uv run uvicorn main:app --port 8000
curl -s localhost:8000/                                   # 原有行为不变
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","payload":{"n":1}}'
curl -s -X POST localhost:8000/tasks/acquire -H 'Content-Type: application/json' \
  -d '{"consumer":"c1","lease_seconds":10}'
```

编程式用法：

```python
from tasklease import TaskStore, TaskRejected, RejectReason

store = TaskStore("tasklease.db", default_lease_seconds=30)
task, created = store.enqueue("order-123", {"url": "..."}, max_attempts=3)
claimed = store.acquire("worker-A")
if claimed:
    try:
        store.complete(claimed.id, "worker-A", {"output": "..."},
                       expected_version=claimed.version)
    except TaskRejected as exc:
        if exc.reason is RejectReason.LEASE_EXPIRED:
            ...  # 租约已过期，交由重新领取
```
