# 本地任务领取与执行持久化存储层 — 设计说明

仅依赖 Python 标准库 `sqlite3`（无外部服务），通过单文件 SQLite（WAL 模式）
为多个并发消费者提供**一致且可解释**的任务状态结论。新能力挂载在 `/api` 前缀下，
原有根路由 `GET /`（`{"Hello": "World"}`）行为完全不变。

## 1. 数据模型（tasks 表）

| 列 | 含义 |
| --- | --- |
| `id` | 任务 UUID（主键） |
| `idempotency_key` | 幂等标识，**唯一索引**，重复提交收敛到同一条 |
| `payload` | 任务载荷 |
| `status` | `PENDING` / `RUNNING` / `SUCCEEDED` / `FAILED` |
| `available_at` | 可领取时间（退避结束时间），`now < available_at` 的 PENDING 不可领取 |
| `lease_owner` | 当前持有者（消费者 ID），无主为 NULL |
| `lease_expires_at` | 租约到期时间 |
| `fencing_token` | 单调递增的 fencing token，每次领取 +1，**执行权以它为准** |
| `attempt_count` | 已被领取的次数（领取即 +1） |
| `max_attempts` | 最大尝试次数（达到后失败即终态） |
| `backoff_base_seconds` / `backoff_factor` | 指数退避参数 |
| `version` | 乐观版本号，每次写入 +1，条件更新必须携带 |
| `result_payload` / `error_message` | 成功结果 / 失败原因 |
| `created_at` / `updated_at` | 时间戳 |

时间来自可注入的 `Clock`：生产用 `SystemClock`（`time.monotonic`，不受墙钟回拨
影响），测试用 `FakeClock` 精确推进。

## 2. 状态机

```
            ┌──────────────────────── release（主动释放，立即可领）──────────────┐
            ▼                                                                   │
PENDING ──claim──▶ RUNNING ──complete──▶ SUCCEEDED（终态）                      │
  ▲                │                                                           │
  │                ├─fail（attempt < max）──▶ PENDING + available_at=now+退避    │
  │                ├─fail（attempt ≥ max）──▶ FAILED（终态，不再被领取）         │
  │                └─租约到期（未显式操作）─▶ 下次 claim 时原子回收为 PENDING ───┘
  └───────────────────────────────────────────────────────────────────────────
```

- `SUCCEEDED` / `FAILED` 为终态，**永远不会再回到可领取状态**。
- “租约过期”不依赖后台扫描：在 `claim` 事务里用条件 UPDATE 原子完成
  `过期 RUNNING → PENDING → RUNNING` 的回收与再领取，因此无需任何调度器。
- 非法流转全部被拒绝并抛出携带 `RejectReason` 的 `StateTransitionError`，
  HTTP 层映射为 409（NOT_FOUND 为 404）。

### 可区分的拒绝原因（RejectReason）

| 原因 | 触发场景 |
| --- | --- |
| `ALREADY_RUNNING` | 幂等键已存在且任务正在执行，重复入队 |
| `ALREADY_TERMINAL` | 幂等键已存在且已 SUCCEEDED/FAILED，重复入队 |
| `NOT_RUNNING` / `NOT_HELD` | 对非 RUNNING 任务做续期/释放/提交 |
| `LEASE_EXPIRED` | 持有者持过期租约做续期/释放/提交结果 |
| `LEASE_HELD_BY_OTHER` | 租约仍有效但 owner 或 fencing token 不匹配 |
| `LEASE_STILL_VALID` | 对仍有效租约做“回收”类操作 |
| `VERSION_CONFLICT` | 携带过期 `expected_version` 的写入 |
| `NOT_FOUND` / `INVALID_PAYLOAD` | 任务不存在 / 入参非法 |

入队结果 `CREATED` 与 `DEDUPED_PENDING` 可区分；领取结果
`ACQUIRED` / `EXPIRED_REACQUIRED` / `EMPTY` 可区分；释放结果
`RELEASED` / `NOT_HELD` / `LEASE_EXPIRED` / `HELD_BY_OTHER` 可区分；
续期结果 `RENEWED` / `NOT_HELD` / `LEASE_EXPIRED` / `HELD_BY_OTHER` 可区分。

## 3. 租约（Lease）与执行权归属

- 领取成功返回 `Lease{task_id, owner, fencing_token, acquired_at, expires_at}`。
- **同一任务任意时刻只能被一个消费者持有**：领取用一条
  `UPDATE ... WHERE status='PENDING' AND available_at<=now AND version=?`
  完成抢占，`rowcount=1` 的唯一消费者获胜，其余得到 `EMPTY`。
- 租约到期后任务可被重新领取：下次 `claim` 会回收过期 RUNNING，并颁发**更大的
  fencing token**；返回 `EXPIRED_REACQUIRED` 明确表示执行权发生过转移。
- 结果提交（complete/fail）同时校验 `owner` 与 `fencing_token`：
  旧持有者即使网络延迟恢复后提交，也会因 token 较小得到 `HELD_BY_OTHER`；
  租约已过期但尚未被他人领取时提交得到 `LEASE_EXPIRED`。
  **持过期租约的消费者不得提交结果。**
- 续期只延长 `lease_expires_at`，不更换 owner/token。

## 4. 失败重试与退避

- `attempt_count` 在每次 claim 时 +1（过期回收重领也算一次新尝试）。
- `fail` 判定：
  - `attempt_count < max_attempts` → 回到 PENDING，
    `available_at = now + backoff_base * backoff_factor^(attempt_count-1)`，
    返回 `RETRY_SCHEDULED`；退避窗口内该任务不会被任何消费者领取。
  - `attempt_count >= max_attempts` → 进入 `FAILED` 终态，返回 `EXHAUSTED`。
- 指数退避示例（base=1s, factor=2）：第 1 次失败等 1s，第 2 次等 2s，第 3 次等 4s。

## 5. 并发约束（如何防止并发覆盖）

1. 写事务一律 `BEGIN IMMEDIATE` 并由进程内锁串行化；跨进程由 SQLite 文件锁 +
   `busy_timeout=5000` + WAL 兜底（已用多进程测试验证）。
2. 所有状态流转都是**单条带完整谓词的条件 UPDATE**（旧状态 + 版本号 + owner +
   fencing token + 到期时间），谓词不匹配则 0 行受影响并给出可区分原因，
   不存在“先读后写”窗口。
3. `complete/fail` 可选携带 `expected_version`，与当前 `version` 不一致时直接
   返回 `VERSION_CONFLICT`，**基于过期版本的写入被拒绝而不是静默生效**。

## 6. 本地数据平滑升级（v0 → v1）

- 版本记录在 `PRAGMA user_version` 与 `schema_meta` 表，`migrate()` 幂等。
- 迁移只做“加表 / 加列 / 加唯一索引”，不删除任何旧表旧列。
- 注意 SQLite 语义：`ALTER TABLE ADD COLUMN` 对**可空列**不会给旧行回填默认值
  （旧行为 NULL）。迁移会显式把遗留 `RUNNING` 行的 `lease_expires_at` 回填为 0
  （语义：无租约信息，视为已过期、无主），使其升级后可被正常回收领取；
  存储层的回收/提交谓词同时用 `COALESCE(...,0)` 做了防御。
- 非空带默认值的列（version、max_attempts、fencing_token 等）由 SQLite 自动
  回填默认值，旧数据升级后可直接读出与执行（见
  `tests/test_upgrade_e2e.py`、`tests/test_schema.py`）。

## 7. 可观测性

`DurableTaskService` 对每次操作打印 INFO 日志，包含：

- 入队标识（idempotency_key）与 CREATED/DEDUPED_PENDING/拒绝判定；
- 租约信息（owner、fencing token、acquired_at、expires_at、剩余秒数）；
- 状态流转判定依据（旧/新状态、attempt x/max、拒绝原因、版本号）。

运行 `pytest -s` 可直接在单测日志中看到上述信息（见 `tests/conftest.py`）。
