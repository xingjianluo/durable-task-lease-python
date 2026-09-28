# 本地任务领取与租约存储层（tasklease）

在现有 FastAPI 服务（`GET /` 行为不变）基础上新增的纯本地持久化任务层。
**无外部服务依赖**：存储基于本地 SQLite 文件（默认 `tasklease.db`，可用环境变量
`TASKLEASE_DB` 覆盖），多个并发消费者（线程或进程）可同时工作，并对任务状态
得到一致且可解释的结论。

代码结构：

| 文件 | 职责 |
| --- | --- |
| `tasklease/models.py` | `Task`/`LeaseGrant` 模型与 `TaskStatus` 状态枚举（含 `quarantined`） |
| `tasklease/errors.py` | 可区分拒绝原因、恢复/归档结果枚举与汇总报告、`TaskRejected` |
| `tasklease/schema.py` | 建表与旧库平滑升级（v3：`lease_token` 列、`tasks_archive`、查询索引） |
| `tasklease/store.py` | 核心存储：凭证化领取/提交、租约、重试、恢复与归档原语 |
| `tasklease/history.py` | 结果/历史只读查询：分页、按状态与时间范围筛选、归档透明读 |
| `tasklease/recovery.py` | 异常运行任务分类（有效/过期/异常）与幂等恢复、隔离 |
| `tasklease/archive.py` | 终态任务归档到 `tasks_archive` |
| `tasklease/cli.py` | 本地运维命令行（恢复扫描/执行、隔离放回、归档） |
| `tasklease/api.py` | 挂载在 `/tasks` 下的 HTTP 接口（可选） |
| `tests/` | 并发领取、凭证轮换、恢复、历史、归档、HTTP 等测试 |

## 1. 状态机

```
                 enqueue(idempotency_key)
                        │
                        ▼
                   ┌─────────┐  acquire()    ┌─────────┐
                   │ PENDING │ ────────────► │ RUNNING │◄─┐
                   └─────────┘                └─────────┘  │
                        ▲                      │  │  │      │
                        │ fail() 未达上限      │  │  │ complete()
                        │ + 退避等待           │  │  ▼      │
                        └──────────────────────┘  │ ┌───────────┐
                                        release() │ │ SUCCEEDED │ ──归档──┐
                                        (主动释放)│ └───────────┘         │
                                                  ▼                       ▼
                                            ┌─────────┐  ┌──────────────┐
                                  fail()    │ FAILED  │─►│ tasks_archive │
                                  达到上限 ► └─────────┘  └──────────────┘
       旧库异常 RUNNING（租约信息缺失）
                        └─运维恢复─► ┌──────────────┐
                                    │ QUARANTINED  │ ──人工确认──► PENDING
                                    └──────────────┘
```

- **PENDING**：可领取（需满足 `available_at <= now`，即退避已结束）。
- **RUNNING**：已被某个消费者持有租约；租约过期后可被其它消费者重新领取。
- **SUCCEEDED / FAILED**：终态。终态任务不再被领取，也不能再提交结果或重复入队；
  可被归档到独立的 `tasks_archive`，归档后状态与产出不变。
- **QUARANTINED**：旧版本升级后租约信息不完整/缺失的 RUNNING 任务被运维隔离，
  不参与领取与提交，等待人工确认后放回 PENDING。

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
| `token_missing` | 操作未携带本次领取的执行凭证（HTTP 层缺失则先返回 422） |
| `token_mismatch` | 凭证不属于当前有效租约世代（旧租约/同名消费者重领后的旧凭证） |
| `task_quarantined` | 任务因租约信息异常已隔离，需人工放回后才能执行 |
| `task_terminal` | 任务已 SUCCEEDED/FAILED，终态不可再变 |
| `version_conflict` | 基于过期版本的写入被拒绝 |
| `task_not_found` | 任务不存在（HTTP 映射为 404，其余为 409） |

拒绝信息会带出任务 id、消费者、当前凭证与所出示凭证的前缀，便于排查
“是谁在什么时候失去执行权”。

## 2. 执行凭证（lease_token）

每次 `acquire()` 成功都会：

1. 为**本次租约世代**随机签发一个不透明凭证 `lease_token`，随 `LeaseGrant` 返回
   （`LeaseGrant` 同时是本次领取的 `Task` 快照，老代码可直接当任务用）；
2. 把它写入任务行。**租约一旦轮换（过期后被他人或同名消费者重新领取、主动释放后
   再领取），存储层立即签发新凭证，旧凭证当场作废。**

消费者在以下操作中必须出示凭证，且只有与当前有效租约一致的凭证才会成功：

- `complete(..., lease_token=...)` 成功提交；
- `fail(..., lease_token=...)` 失败上报；
- `heartbeat(..., lease_token=...)` 续期；
- `release(..., lease_token=...)` 主动释放。

安全语义：

- **同名消费者在租约过期后重新领到同一任务，上一次拿到的凭证立即作废**：
  不能提交结果/失败、不能续期或释放，更不能覆盖后来执行的产出（`token_mismatch`）。
- 凭证只做身份证明，不能绕过租约有效期与乐观版本检查：
  判定顺序为 终态 → 未领取 → 已隔离 → 持有者不符 → 租约过期 → 凭证缺失/作废 → 版本冲突。
- 条件 UPDATE 也带 `lease_token` 条件，即使检查与写入之间发生并发轮换，
  旧凭证写入同样 `rowcount=0` 被拒。

> 兼容说明：存储层方法的 `lease_token` 为可选关键字参数（不传时退化为仅按
> 消费者+租约时间判定），但 HTTP 接口对 complete/fail/heartbeat/release
> **强制要求**凭证（`min_length=1`），推荐所有新调用方一律传凭证。

## 3. 租约管理

- `acquire(consumer, lease_seconds)` 用单条条件 `UPDATE ... WHERE rowid =
  (SELECT ... LIMIT 1)` 原子地选取并占用任务：候选为“可领取的 PENDING”或
  “租约已到期且租约信息完整的 RUNNING”。因此**同一任务任意时刻只有一个消费者
  持有**，并写入 `lease_owner`、`lease_expires_at`、`lease_token`，执行权归属明确。
  租约信息不完整（任一为 NULL）的 RUNNING 行**不会**被领取流程接管，
  交由第 5 节的恢复入口处理，避免误执行历史脏行。
- 租约判定：`lease_expires_at > now` 有效；`<= now` 即过期（到期点本身视为过期）。
- `heartbeat(task_id, consumer, lease_seconds, lease_token)` 续期，返回四态：
  - `renewed`：持有效凭证的持有者续期成功（版本号 +1）；
  - `not_held`：任务不在运行中或调用者不是持有者；
  - `expired`：租约已过期，续期被拒绝；
  - `token_mismatch`：调用者虽是同名持有者，但凭证属于旧租约世代（或凭证为空）。
- `release(task_id, consumer, lease_token)` 释放，返回四态：
  - `released`：持有效凭证的持有者主动释放，任务回到 PENDING 且立即可重新领取；
  - `not_held`：非持有者或任务不在运行中；
  - `already_expired`：租约已过期，不改写状态，等待领取流程重新分配；
  - `token_mismatch`：凭证不属于当前租约世代（同名消费者的旧凭证也不行）。
- **持过期租约或旧凭证的消费者不能提交结果**：`complete/fail` 都会先校验
  租约归属、有效期与执行凭证。

## 4. 失败重试与退避

- 每次 `acquire` 都会令 `attempt += 1`。
- `fail()` 时：
  - 若 `attempt < max_attempts`：任务回到 PENDING，按指数退避设置
    `available_at = now + backoff`（默认 `backoff_base * 2**(attempt-1)`，
    也可显式传 `backoff_seconds`）；退避窗口内任务不可被领取。
  - 若 `attempt >= max_attempts`：进入终态 FAILED，永不再被领取。
- `complete()` 进入终态 SUCCEEDED，清除租约。

## 5. 并发约束与防覆盖

- **进程内**：所有会话经一把可重入锁串行化，连接设置了 `busy_timeout`。
- **跨进程**：SQLite（WAL 模式）写事务天然串行化；领取/释放/续期/提交全部使用
  带状态、持有者、版本、租约到期、执行凭证条件的 UPDATE，冲突时 `rowcount=0`，
  不会静默覆盖。
- **执行凭证防覆盖**：条件 UPDATE 在调用方提供凭证时附加 `AND lease_token=?`，
  即使租约校验通过后、写入前的瞬间发生租约轮换，旧世代凭证的写入仍会被原子拒绝。
- **乐观版本控制**：每行有单调递增的 `version`，每次状态变更 +1。
  `complete/fail` 可携带 `expected_version`，与当前版本不符时拒绝
  （`version_conflict`），而不是让过期写入静默生效。
- 所有关键流转写入 `task_events` 审计表（事件、操作者、from/to 状态、版本、原因），
  保证结论“可解释”。

## 6. 旧数据平滑升级与异常运行任务恢复

### 6.1 平滑升级（schema v3）

`schema.initialize()` 以 `PRAGMA user_version` 记录结构版本：

- 全新库直接建最新结构；
- 已存在的旧 `tasks` 表（例如只有 `id,status` 两列）通过逐列
  `ALTER TABLE ADD COLUMN` 补齐，缺失列填兼容默认值（空幂等键、`'{}'` 负载、
  `attempt=0, version=0, available_at=0` 等），**升级后旧数据仍可正常读取并
  继续走完生命周期**；
- v3 新增的 `lease_token` 对历史 RUNNING 行为 **NULL（不伪造凭证）**，
  这正是区分“租约信息异常需要恢复”的信号；v3 同时建立 `tasks_archive` 归档表、
  状态/事件查询索引；
- 幂等唯一索引只约束**非空**幂等键（部分索引），因此历史空键行不会导致升级失败。

### 6.2 三类可区分的运行状态（`RecoveryState`）

对每条 RUNNING 任务可通过 `RecoveryService.classify(task)`（或 `recovery scan`）分类：

| 分类 | 判定条件 | 处置 |
| --- | --- | --- |
| `active` | `lease_owner`/`lease_expires_at`/`lease_token` 均非空且未过期 | **绝不触碰**，恢复可重复运行也不影响它 |
| `expired` | 租约信息完整但 `lease_expires_at <= now` | 恢复为 PENDING，立即可重新领取 |
| `corrupt` | 三个租约字段任一为 NULL（典型为旧版本升级后的运行行） | 默认隔离为 QUARANTINED 等人工，或按策略直接放回 |

### 6.3 运维入口（编程式 / CLI / HTTP）

- 只读扫描：`RecoveryService(store).scan()` → `RecoveryReport`
  （`scanned_running/active/expired/corrupt` 计数），不改任何数据。
- 仅恢复过期：`recover_expired(limit=...)`，只动 `expired`，不碰 active/corrupt。
- 全量恢复：`recover_all(requeue_corrupt=False, limit=...)`：
  - 默认把 corrupt 行置为 **QUARANTINED**（清掉残缺租约、版本 +1、写审计事件），
    等待人工 `requeue_quarantined(task_id)` 确认后放回 PENDING；
  - 显式 `requeue_corrupt=True` 时把 corrupt 行直接放回 PENDING。
- 逐条动作结论见 `RecoveryAction`（如 `skipped_active`、`requeued_expired`、
  `quarantined_corrupt`、`requeued_corrupt`、以及各种 `*_already_*` 幂等结论）。

幂等与不阻塞保证：

- 每条恢复都是**带状态/版本/租约条件的独立小事务**：扫描后、写入前若任务已被
  正常领取或提交，条件 UPDATE `rowcount=0`，不误伤正在正常执行的任务；
- 重复执行：第二次扫描时这些行已不在 RUNNING 集合，计数归零、无额外副作用；
- 恢复与正常领取/提交共用同一把短锁，扫描只读且不持长锁，恢复期间正常流程不被阻塞。

```bash
python -m tasklease.cli --db tasklease.db recovery scan          # 只分类
python -m tasklease.cli --db tasklease.db recovery run           # 过期恢复 + 异常隔离
python -m tasklease.cli --db tasklease.db recovery run --requeue-corrupt
python -m tasklease.cli --db tasklease.db recovery requeue <id>  # 人工放回
```

## 7. 执行结果与历史查询（`HistoryService`，只读）

- `get_result(task_id) -> TaskResult`：读回**最终状态、执行产出 `result`、
  失败原因 `last_error`、尝试次数、最终租约归属、时间戳**；主表查不到时
  自动回退归档表，归档任务同样可读。任务不存在抛 `task_not_found`。
- `list_events(task_id=None, statuses=None, start=None, end=None, limit=50, offset=0)
  -> Page[TaskEventRecord]`：查询**尝试过程与租约归属变化**（领取/续期/释放/
  成功/失败/恢复/归档等事件，含操作者、from/to 状态、版本、原因、时间）。
  - `statuses` 按事件的 `to_status` 过滤（如 `succeeded`、`failed`）；
  - `start/end` 是事件时间闭区间；`limit 1..1000`、`offset>=0`，非法值报错；
  - 返回 `Page(items,total,limit,offset,next_offset)`，`next_offset` 为 None 表示末页。
- `list_tasks(statuses=None, start=None, end=None, limit, offset, include_archived=False)
  -> Page[Task]`：按**任务状态与更新时间范围**分页筛选；默认只查主表，
  `include_archived=True` 时合并归档表并按时间统一排序，归档记录 `Task.archived=True`。
- 所有查询只读，不阻塞领取与提交；事件表不随归档删除，归档后完整尝试过程仍可查。

## 8. 终态任务归档（`ArchiveService`）

- **归档范围**：仅 `SUCCEEDED/FAILED`；运行中/PENDING/QUARANTINED 一律跳过
  （`archive_one` 返回 `skipped_non_terminal`）。
- `archive_terminal(older_than_seconds=None, limit=500)`：可要求
  `updated_at <= now-older_than_seconds` 才归档，给刚终结的任务留出观察窗口；
  `limit` 控制单批数量。
- 迁移方式：候选只读选取 + **逐条独立小事务**（插入 `tasks_archive` 后从主表
  删除），归档过程不会长时间持锁，**不挡新任务的领取与提交**；可分批反复跑到收敛。
- **兼容范围（归档后语义不回归）**：
  - `get(task_id)`、`get_result` 透明读回归档表，最终状态与 `result/last_error` 完整；
  - 归档表对非空幂等键同样有唯一索引，终态任务重复入队仍返回
    `task_terminal` 拒绝，历史既有结论不变；
  - 历史事件不迁移、不删除，仍可按任务 id 查询完整尝试过程；
  - 归档动作幂等：重复运行主表已无候选（计数为 0），
    `archive_one` 对已归档任务返回 `already_archived`，不会产生重复行。

```bash
python -m tasklease.cli --db tasklease.db archive run --older-than 86400 --limit 1000
python -m tasklease.cli --db tasklease.db archive one <id>
```

## 9. HTTP 接口（与原服务共存）

挂载在 `/tasks` 前缀下，原有 `GET / -> {"Hello":"World"}` 保持不变：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /tasks` | 幂等入队，body：`idempotency_key, payload, max_attempts` |
| `GET  /tasks` | 任务分页列表，query：`status,start,end,limit,offset,include_archived` |
| `POST /tasks/acquire` | 领取，body：`consumer, lease_seconds?`；返回 `lease_token` 与任务快照 |
| `POST /tasks/{id}/complete` | 成功提交，body：`consumer, lease_token(必填), result?, expected_version?` |
| `POST /tasks/{id}/fail` | 失败上报，body：`consumer, lease_token(必填), error?, backoff_seconds?, expected_version?` |
| `POST /tasks/{id}/heartbeat` | 租约续期，body：`consumer, lease_seconds?, lease_token(必填)` |
| `POST /tasks/{id}/release` | 主动释放，body：`consumer, lease_token(必填)` |
| `GET  /tasks/{id}` | 查询任务（归档任务透明读回，带 `archived` 标记） |
| `GET  /tasks/{id}/result` | 读回最终状态、产出与失败原因（含归档） |
| `GET  /tasks/events` | 历史事件分页，query：`task_id,status,start,end,limit,offset` |
| `POST /tasks/recovery/scan` | 只读分类扫描 RUNNING 任务 |
| `POST /tasks/recovery/run` | 恢复，body：`requeue_corrupt?, limit?` |
| `POST /tasks/{id}/requeue-quarantined` | 人工把隔离任务放回 PENDING |
| `POST /tasks/archive/run` | 批量归档，body：`older_than_seconds?, limit?` |
| `POST /tasks/{id}/archive` | 归档指定任务 |

非法操作返回 `409`（任务不存在为 `404`，缺少必填凭证等请求体问题为 `422`），
响应形如：

```json
{"detail": {"reason": "token_mismatch", "message": "任务 ... 的执行凭证已轮换：消费者 ... 出示的凭证 ... 不属于当前有效租约世代 ..."}}
```

## 10. 本地验证

```bash
# 安装依赖（uv）
uv sync

# 运行全部单测（日志会打印入队标识、租约信息与状态流转判定依据）
uv run pytest -q
uv run pytest -s -o log_cli=true          # 查看详细判定日志

# 新增能力对应的测试文件
uv run pytest tests/test_token.py        # 凭证轮换即作废、同名重领旧凭证不能提交
uv run pytest tests/test_recovery.py     # 旧库异常运行行分类/恢复/隔离/幂等
uv run pytest tests/test_history.py      # 结果读回、尝试过程、归属变化、分页筛选
uv run pytest tests/test_archive.py      # 归档范围、透明读回、幂等入队结论不回归
uv run pytest tests/test_api_http.py     # HTTP 层凭证强制与运维/查询端点

# 运维命令行（在项目目录内执行）
python -m tasklease.cli --db tasklease.db recovery scan
python -m tasklease.cli --db tasklease.db recovery run
python -m tasklease.cli --db tasklease.db archive run --older-than 86400

# 启动 HTTP 服务并手动验证
uv run uvicorn main:app --port 8000
curl -s localhost:8000/                                   # 原有行为不变
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","payload":{"n":1}}'
curl -s -X POST localhost:8000/tasks/acquire -H 'Content-Type: application/json' \
  -d '{"consumer":"c1","lease_seconds":10}'
# -> {"acquired":true,"task":{...},"lease_token":"<只对本次执行有效的凭证>"}
curl -s -X POST localhost:8000/tasks/<id>/complete -H 'Content-Type: application/json' \
  -d '{"consumer":"c1","lease_token":"<上一步凭证>","result":{"output":"..."}}'
```

编程式用法：

```python
from tasklease import (
    ArchiveService, HistoryService, RecoveryService,
    TaskRejected, RejectReason, TaskStore,
)

store = TaskStore("tasklease.db", default_lease_seconds=30)
task, created = store.enqueue("order-123", {"url": "..."}, max_attempts=3)
grant = store.acquire("worker-A")
if grant:
    try:
        # 提交/续期/释放都必须带上本次领取的凭证
        store.heartbeat(grant.id, "worker-A", lease_token=grant.lease_token)
        store.complete(grant.id, "worker-A", {"output": "..."},
                       expected_version=grant.version,
                       lease_token=grant.lease_token)
    except TaskRejected as exc:
        if exc.reason is RejectReason.LEASE_EXPIRED:
            ...  # 租约已过期，交由重新领取
        elif exc.reason is RejectReason.TOKEN_MISMATCH:
            ...  # 凭证已轮换：本次执行权已丧失，不得再写结果

# 运维：先扫描看分类，再恢复（幂等，可重复执行）
print(RecoveryService(store).scan())
RecoveryService(store).recover_all()

# 查询：结果、尝试过程、租约归属变化（分页 + 状态/时间筛选）
hist = HistoryService(store)
print(hist.get_result(task.id))
hist.list_events(task.id, statuses=["succeeded", "failed"], limit=50)

# 归档：终结超过 1 天的终态任务，归档后结论与读回不变
ArchiveService(store).archive_terminal(older_than_seconds=86400)
```
