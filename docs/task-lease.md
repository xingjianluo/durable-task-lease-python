# 本地任务领取与租约存储层（tasklease）

在现有 FastAPI 服务（`GET /` 行为不变）基础上的纯本地持久化任务层。
**无外部服务依赖**：存储基于本地 SQLite 文件（默认 `tasklease.db`，可用环境变量
`TASKLEASE_DB` 覆盖），多个并发消费者（线程或进程）可同时工作，并对任务状态
得到一致且可解释的结论。

代码结构：

| 文件 | 职责 |
| --- | --- |
| `tasklease/models.py` | `Task`/`LeaseGrant`/`TaskEvent`/`ResultView`/`Page` 模型与 `TaskStatus` |
| `tasklease/errors.py` | 可区分拒绝原因、恢复/健康分类、各类结果枚举与 `TaskRejected` |
| `tasklease/schema.py` | 建表与旧库平滑升级（`PRAGMA user_version`，当前结构版本 v3） |
| `tasklease/store.py` | 核心存储：执行凭证、幂等入队、并发领取、租约、重试、恢复、查询、归档 |
| `tasklease/api.py` | 挂载在 `/tasks` 下的 HTTP 接口（含 `/admin` 运维入口，可选） |
| `tests/` | 凭证轮换、异常恢复、结果/历史查询、归档、并发接管等测试 |

## 1. 状态机

```
                 enqueue(idempotency_key)
                        │
                        ▼
                   ┌─────────┐  acquire_grant() ┌─────────┐
                   │ PENDING │ ───────────────► │ RUNNING │
                   └─────────┘                   └─────────┘
                        ▲                      │  │  │  │
                        │ fail() 未达上限       │  │  │  │ complete()
                        │ + 退避等待            │  │  │  ▼
                        └──────────────────────┘  │  │ ┌───────────┐
                                        release() │  │ │ SUCCEEDED │
                                        (主动释放)│  │ └───────────┘
                                                  ▼  ▼ ┌─────────┐
                                              recover │ │ FAILED  │（重试耗尽）
                                              MANUAL  ►└─────────┘
                                                  ▼
                                            ┌─────────────┐
                                            │ DEAD_LETTER │（人工处理）
                                            └─────────────┘
```

- **PENDING**：可领取（需满足 `available_at <= now`）。
- **RUNNING**：被某消费者持有租约；过期后可被重新领取（凭证随之轮换）。
- **SUCCEEDED / FAILED**：正常终态，可被归档。
- **DEAD_LETTER**：运维把“租约信息异常”的运行任务标记为人工处理；不再被自动
  领取、不能提交结果、重复入队返回 `task_dead_letter`。

## 2. 执行凭证（fence token）

每次成功领取都会签发一个**只属于本次执行**的随机凭证 `lease_token`，通过
`acquire_grant()` 的返回值 `LeaseGrant(task, token)` 交给消费者：

```python
grant = store.acquire_grant("worker-A", lease_seconds=30)
if grant is None:
    ...  # 暂无任务
task, token = grant.task, grant.token
store.complete(task.id, "worker-A", {"output": "..."}, token=token)
# fail / heartbeat / release 同样都要回传 token
```

规则：

- **只有“当前有效租约 + 凭证匹配”才能写成功**。租约发生任何轮换（重新领取、
  过期接管、主动释放、运维恢复）都会换新凭证，旧凭证从轮换那一刻起立即失效，
  不能提交任何结果，也无法覆盖后来执行的产出。
- **同名消费者重新领取同一任务也一样**：即使 `lease_owner` 仍是自己，上一轮
  执行拿旧凭证提交会被拒绝（`lease_token_mismatch`），新一轮产出受保护。
- 凭证以 `hmac.compare_digest` 常量时间比较；租约终结/交出后凭证列清空。
- 出于最小暴露原则，普通 `get/list` 查询**不回读凭证**，凭证只在领取回执中给出。
- 兼容旧调用：`complete/fail/heartbeat/release` 的 `token` 参数可选；不传时按
  “owner + 租约有效期”判定（用于平滑升级）。新代码应当始终传凭证。
  `acquire()` 旧签名仍可用，但只返回任务快照，建议改用 `acquire_grant()`。

### “丢掉执行权”的三类可区分拒绝结论

按以下顺序判定，结论互不混淆：

| `RejectReason` | 含义 | 典型场景 |
| --- | --- | --- |
| `lease_expired` | 租约已过期，还没被别人接管 | 自己卡住超过 TTL 后提交 |
| `lease_held_by_other` | 执行权已被别人接管（owner 不同） | 过期后被其他消费者领走 |
| `lease_token_mismatch` | owner 是自己且租约有效，但凭证对不上 | 同名消费者重领后旧执行提交 |

另有：

| 原因 | 含义 |
| --- | --- |
| `lease_malformed` | 租约字段缺失/矛盾，需先走恢复入口 |
| `task_terminal` | 已 SUCCEEDED/FAILED |
| `task_dead_letter` | 已标记人工处理 |
| `not_pending_or_unclaimed` | PENDING 未领取就提交 |
| `version_conflict` | 基于过期 `expected_version` 的写入 |
| `task_not_found` | 任务不存在（HTTP 404，其余拒绝 HTTP 409） |

续期 `heartbeat()` / 释放 `release()` 的返回也可区分：

- heartbeat：`renewed` / `not_held`（非持有者或非运行中）/ `expired` /
  `invalid_token`（凭证对不上）。
- release：`released` / `not_held` / `already_expired` / `invalid_token`。

## 3. 租约、失败重试与并发防覆盖

- `acquire_grant(consumer, lease_seconds)` 用单条条件
  `UPDATE ... WHERE rowid=(SELECT ... LIMIT 1)` 原子占用：候选为“可领取的
  PENDING”或“**租约字段齐全且**已到期的 RUNNING”。租约字段异常的 RUNNING
  不会被静默接管，统一交恢复入口（见第 4 节）。
- 到期判定：`lease_expires_at > now` 有效；`<= now` 即过期（到期点算过期）。
- 每次 `acquire` 令 `attempt += 1`；`fail()` 在未达 `max_attempts` 时回到
  PENDING 并按指数退避设置 `available_at`，达到上限进入终态 FAILED。
- 所有写操作使用带状态、持有者、凭证、版本、到期条件的 UPDATE，冲突时
  `rowcount=0`，不会静默覆盖；`version` 单调递增，可带 `expected_version`
  做乐观并发控制。
- 每次关键流转（含 `acquire` 的凭证指纹、恢复动作）都写入 `task_events`。

## 4. 异常运行任务的恢复（运维入口）

旧版本升级上来的 RUNNING 行可能缺 `lease_owner` / `lease_expires_at`
（甚至完全没有租约列），会永久卡在运行中、再不会被领取。先体检再恢复：

```python
store.inspect_lease_health(task_id)
# LeaseHealth.HELD       —— 字段齐全且租约未过期：仍被有效持有，禁止动它
# LeaseHealth.EXPIRED    —— 字段齐全但已过期：可安全恢复
# LeaseHealth.MALFORMED  —— owner/到期时间缺失或空串：租约信息异常
# LeaseHealth.NOT_RUNNING—— 任务不在运行中
```

恢复（两种策略，可按 id 列表或批量扫描，逐条小事务提交）：

```python
# 1) 恢复为可重新领取（attempt 不增加，旧凭证全部失效）
report = store.recover_running(RecoveryAction.REQUEUE)             # 批量
store.recover_running(RecoveryAction.REQUEUE, task_ids=[id])       # 指定
# 2) 标记为 DEAD_LETTER，等待人工处理
store.recover_running(RecoveryAction.MANUAL, task_ids=[id])
```

返回 `RecoveryReport(scanned, items)`，每条给出 `result`
（`requeued` / `marked_manual` / `skipped_held` / `skipped_not_running`）
与体检时的 `health`，便于看清“是谁、在什么时候、因为什么丢掉执行权”。

恢复语义保证：

- **只动 EXPIRED / MALFORMED**；条件 UPDATE 在写入时再次校验
  `lease_owner/lease_expires_at` 与到期时刻，**绝不会把正在正常执行的任务
  （HELD）改掉**。
- **可反复执行、无额外副作用**：已恢复/终结的任务再次扫描得到
  `skipped_not_running`，版本与状态不再变化。
- **不阻塞正常流量**：逐条独立小事务提交（WAL），恢复期间领取与提交照常进行。
- HTTP：`GET /tasks/{id}/lease-health`、
  `POST /tasks/admin/recover-running`（body：`action`、`task_ids?`、`limit?`）。

## 5. 结果与执行历史查询

| 方法 | 说明 |
| --- | --- |
| `get_result(task_id)` | 终态结果读回：`status/result/last_error/attempt/max_attempts/时间戳/archived`（含归档任务） |
| `list_events(...)` | 执行历史分页查询，可按 `task_id / statuses / actor / since / until` 过滤 |
| `list_tasks(...)` | 任务分页查询，可按 `statuses / since / until` 过滤，`include_archived` 决定是否含归档区 |
| `get(id)` / `get_by_idempotency_key(k)` | 默认连归档区一起查，返回对象带 `archived/archived_at` |

分页统一用不透明游标：返回 `Page(items, next_cursor)`，把 `next_cursor`
原样作为下一次调用的 `cursor` 传入；为 `None` 表示没有更多。状态过滤对事件
同时匹配 from/to（流转进入或离开该状态都算命中）。非法 cursor 抛
`ValueError`（HTTP 层返回 400）。

`task_events` 记录每次 `enqueue/acquire/complete/fail/heartbeat/release` 与
`recover.*` 运维动作，携带 actor、from/to、version、原因与本次租约的凭证指纹，
因此可以看清一次任务的尝试过程以及租约归属是如何轮换的。

HTTP：

- `GET /tasks/{id}/result`
- `GET /tasks/events?status=succeeded&since=...&until=...&actor=...&task_id=...&limit=&cursor=`
- `GET /tasks?status=failed&since=&until=&include_archived=&limit=&cursor=`

## 6. 终态归档

任务量大时，把**终结已久的正常终态任务**搬到独立归档区：

```python
store.archive_terminal(older_than=7*24*3600, limit=500)
# -> ArchiveReport(archived, scanned, task_ids)
```

归档范围与兼容范围：

- **只归档 `SUCCEEDED` / `FAILED`**；`DEAD_LETTER` 属人工处理范围，不自动归档；
  非终态任务绝不移动。
- 任务行与其**全部执行历史**整体搬到 `tasks_archived` /
  `task_events_archived`，记录 `archived_at`。
- **归档后仍可读**：`get/get_result/get_by_idempotency_key` 默认透明查归档区，
  `list_events/list_tasks` 用 `include_archived=True` 可跨活跃/归档两区翻页
  （不重不漏）。
- **既有幂等结论不回归**：幂等结论权威表 `task_keys` 对已归档键保留
  `archived=1` 行（永不删除）。归档后用同一 `idempotency_key` 重复入队依旧被
  `task_terminal` 拒绝，不会产生新任务、不会覆盖老结论。
- **归档不挡新流量**：逐条小事务搬迁，过程中新任务领取/提交照常；归档本身
  幂等，重复执行对已归档任务是 no-op。
- HTTP：`POST /tasks/admin/archive-terminal`
  （body：`older_than_seconds?`、`limit?`）。

## 7. 旧数据平滑升级

`schema.initialize()` 以 `PRAGMA user_version` 记录结构版本，当前 **v3**：

- 全新库直接建最新结构；
- 旧库逐表逐列 `ALTER TABLE ADD COLUMN` 补齐（v3 新增 `tasks.lease_token`、
  `task_events.lease_token`），旧行凭证为 NULL，语义等价于“无凭证的历史租约”；
- 自动建立 `tasks_archived`、`task_events_archived` 与幂等结论表 `task_keys`，
  并从 tasks/归档表回填 `task_keys`（`INSERT OR IGNORE`，可重复执行）；
- 幂等唯一索引只约束非空键，历史空键行不会导致升级失败；
- 升级后旧的卡住 RUNNING 行通过第 4 节的恢复入口处理。

## 8. HTTP 接口（与原服务共存）

挂载在 `/tasks` 前缀下，原有 `GET / -> {"Hello":"World"}` 不变：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /tasks` | 幂等入队 `idempotency_key, payload?, max_attempts?` |
| `POST /tasks/acquire` | 领取，返回 `task` 与 **`lease_token`** |
| `POST /tasks/{id}/complete` | `consumer, result?, expected_version?, lease_token?` |
| `POST /tasks/{id}/fail` | `consumer, error?, backoff_seconds?, expected_version?, lease_token?` |
| `POST /tasks/{id}/heartbeat` | `consumer, lease_seconds?, lease_token?` |
| `POST /tasks/{id}/release` | `consumer, lease_token?` |
| `GET  /tasks/{id}` | 查询任务（默认含归档） |
| `GET  /tasks/{id}/result` | 读回终态结果/产出/失败原因 |
| `GET  /tasks/{id}/lease-health` | 租约体检：held/expired/malformed/not_running |
| `GET  /tasks/events` | 执行历史分页+过滤 |
| `GET  /tasks` | 任务分页+过滤 |
| `POST /tasks/admin/recover-running` | 异常运行任务恢复 |
| `POST /tasks/admin/archive-terminal` | 终态归档 |

非法操作返回 `409`（任务不存在 `404`，非法分页/状态参数 `400`）：

```json
{"detail": {"reason": "lease_token_mismatch",
            "message": "任务 ... 的执行凭证与当前有效租约不符 ..."}}
```

## 9. 本地验证

```bash
# 安装依赖（uv）
uv sync

# 运行全部单测（共 50+ 个；日志打印入队标识、租约与状态流转判定依据）
uv run pytest -q
uv run pytest -s -o log_cli=true                 # 查看详细判定日志
uv run pytest tests/test_fence_token.py -q       # 只跑凭证轮换/同名接管
uv run pytest tests/test_recovery.py -q          # 只跑异常恢复
uv run pytest tests/test_history_query.py -q     # 只跑结果/历史查询
uv run pytest tests/test_archive.py -q           # 只跑归档
uv run pytest tests/test_api_http.py -q          # 只跑 HTTP 接口

# 启动 HTTP 服务并手动验证
uv run uvicorn main:app --port 8000
curl -s localhost:8000/                                   # 原有行为不变
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","payload":{"n":1}}'
curl -s -X POST localhost:8000/tasks/acquire -H 'Content-Type: application/json' \
  -d '{"consumer":"c1","lease_seconds":30}'
# -> 记下返回的 lease_token，后续操作都带上它
curl -s -X POST localhost:8000/tasks/<id>/complete -H 'Content-Type: application/json' \
  -d '{"consumer":"c1","result":{"ok":true},"lease_token":"<token>"}'
```

编程式用法：

```python
from tasklease import (
    TaskStore, TaskRejected, RejectReason, RecoveryAction, LeaseHealth,
)

store = TaskStore("tasklease.db", default_lease_seconds=30)
task, created = store.enqueue("order-123", {"url": "..."}, max_attempts=3)

grant = store.acquire_grant("worker-A")
if grant:
    try:
        store.complete(grant.id, "worker-A", {"output": "..."}, token=grant.token)
    except TaskRejected as exc:
        if exc.reason is RejectReason.LEASE_EXPIRED:
            ...          # 自己租约过期，任务会被重新领取
        elif exc.reason is RejectReason.LEASE_HELD_BY_OTHER:
            ...          # 已被别人接管
        elif exc.reason is RejectReason.LEASE_TOKEN_MISMATCH:
            ...          # 自己名下的新一轮执行已开始，旧凭证作废

# 运维：把卡住的异常 RUNNING 恢复成可领取，HELD 的一律跳过
report = store.recover_running(RecoveryAction.REQUEUE)
for item in report.items:
    print(item.task_id, item.health.value, item.result.value)

# 查询结果/历史
print(store.get_result(grant.id))
page = store.list_events(statuses=["succeeded"], since=..., limit=50)

# 归档一周前终结的任务
store.archive_terminal(older_than=7 * 24 * 3600, limit=500)
```
