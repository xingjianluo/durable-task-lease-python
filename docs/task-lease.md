# 本地任务领取与租约存储层（tasklease）

在现有 FastAPI 服务（`GET /` 行为不变）基础上新增的纯本地持久化任务层。
**无外部服务依赖**：存储基于本地 SQLite 文件（默认 `tasklease.db`，可用环境变量
`TASKLEASE_DB` 覆盖），多个并发消费者（线程或进程）可同时工作，并对任务状态
得到一致且可解释的结论。

本版在基础领取/租约能力之上补齐了四块能力：

1. **执行凭证（lease token）**：每次领取发放只对本次执行有效的凭证，续期、
   提交成功/失败、释放都必须携带；租约一轮换旧凭证立刻失效，旧执行无法覆盖
   新一轮产出。
2. **异常 RUNNING 任务恢复**：旧版本升级遗留的缺字段“卡死”任务可被发现、
   三分类，并幂等地恢复为可重新领取或隔离待人工处理。
3. **结果与执行历史读回**：成功产出、失败原因、逐次尝试与租约归属变化可查询，
   支持分页与按状态、时间范围筛选。
4. **终态任务归档**：终结很久的任务搬到独立归档表，归档后最终状态/产出/历史
   仍可读，重复入队的既有结论不变，归档不挡新任务的领取与提交。

代码结构：

| 文件 | 职责 |
| --- | --- |
| `tasklease/models.py` | `Task`/`EventRecord`/`Page` 模型与 `TaskStatus` 状态枚举 |
| `tasklease/errors.py` | 可区分的拒绝原因、各类操作结果/分类枚举与 `TaskRejected` |
| `tasklease/schema.py` | 建表与旧库平滑升级（`PRAGMA user_version`，当前结构版本 v3） |
| `tasklease/store.py` | 核心存储：凭证租约、幂等入队、并发领取、重试、恢复、查询、归档 |
| `tasklease/api.py` | 挂载在 `/tasks` 下的 HTTP 接口（可选） |
| `tasklease/cli.py` | 本地运维命令行：`stuck` / `recover` / `requeue` / `archive` |
| `tests/` | 凭证轮换、恢复、历史读回、归档、并发与旧库升级等测试 |

## 1. 状态机

```
                 enqueue(idempotency_key)
                        │
                        ▼
                   ┌─────────┐  acquire()    ┌───────────┐
                   │ PENDING │ ────────────► │  RUNNING  │
                   └─────────┘                └───────────┘
                        ▲                      │  │  │  │
                        │ fail() 未达上限      │  │  │  ├─► QUARANTINED（旧库租约异常，
                        │ + 退避等待           │  │  │  │    运维恢复隔离，等待人工）
                        └──────────────────────┘  │  │  │
                                        release() │  │  ▼
                                        (主动释放)│  │ ┌───────────┐
                                                  │  │ │ SUCCEEDED │
                                                  │  ▼ └───────────┘
                                                  │ ┌─────────┐
                                  fail() 达到上限 ► │ FAILED  │ （终态：重试耗尽）
                                                    └─────────┘
```

- **PENDING**：可领取（需满足 `available_at <= now`，即退避已结束）。
- **RUNNING**：已被某个消费者持有租约；租约过期后可被其它消费者重新领取。
- **SUCCEEDED / FAILED**：终态。终态任务不再被领取，也不能再提交结果或重复入队。
- **QUARANTINED**：旧版本升级遗留、租约信息缺失的 RUNNING 任务被恢复入口隔离
  后的状态，等待人工确认；不是终态，可用 `requeue_quarantined` 放回队列。

### 入队约束（幂等 + 状态机）

- 同一 `idempotency_key` 的任意次提交都收敛到**同一条任务**，数据库对非空键有
  唯一索引，即使并发提交也不会产生重复记录（返回 `CREATED` 或幂等命中 `EXISTING`）。
- 任务**正在执行且租约有效**时重复入队 → 拒绝，原因 `task_executing`。
- 任务已处于**终态**（含已归档终态）时重复入队 → 拒绝，原因 `task_terminal`。
- RUNNING 但**租约信息缺失**（旧库遗留）时重复入队 → 拒绝，原因 `lease_corrupt`，
  提示先走恢复入口。
- 任务已被隔离时重复入队 → 拒绝，原因 `task_quarantined`。
- 租约已过期的 RUNNING 任务允许重复提交，但仍收敛到同一任务（等待重新领取）。

## 2. 执行凭证（lease token）

### 2.1 基本用法

`acquire(consumer, lease_seconds)` 成功时返回 `(Task, lease_token)`。
**lease_token 是一串只属于本次执行的随机凭证**（UUID hex），同时落在任务行的
`lease_token` 列。之后每次写操作都必须出示它：

```python
claimed = store.acquire("worker-A", lease_seconds=30)
if claimed:
    task, token = claimed
    store.heartbeat(task.id, "worker-A", token, lease_seconds=30)   # 续期
    store.complete(task.id, "worker-A", token, {"output": "..."},
                   expected_version=task.version)                    # 成功
    # 或 store.fail(task.id, "worker-A", token, "error ...")
    # 或 store.release(task.id, "worker-A", token)                   # 主动释放
```

- 每次 `acquire`（含过期接管、释放后重领、失败重试后重领）都生成**全新 token**。
- complete / fail / release 生效后租约字段（owner/expires/token）清空。
- token 随每次 `acquire`、`heartbeat`、`release`、`complete`、`fail` 事件
  记入 `task_events.lease_token`，可据此串起每一次尝试的完整归属轨迹。

### 2.2 为什么必须有凭证

仅凭消费者名校验有一个致命漏洞：同名消费者在租约过期后重新领到同一个任务时，
**上一轮卡住的执行**仍然“看起来是持有者”，它迟到的成功提交会覆盖新一轮产出，
而真正的新一轮提交反被当成重复。引入一次性凭证后：

- 租约轮换的**那一刻**旧 token 立即失效，旧执行的 complete/fail/heartbeat/release
  全部被拒，且不产生任何写入，新一轮产出不会被覆盖。
- 拒绝原因可区分，能明确看出“是谁、在什么时候、因为什么丢掉了执行权”。

### 2.3 可区分的拒绝原因（complete/fail）

判定按固定顺序进行，结论互不混淆：

| 顺序 | 场景 | `RejectReason` |
| --- | --- | --- |
| 1 | 任务已 SUCCEEDED/FAILED（含已归档终态） | `task_terminal` |
| 1 | 任务已被隔离 | `task_quarantined` |
| 1 | 任务不是 RUNNING（未领取） | `not_pending_or_unclaimed` |
| 2 | RUNNING 但 owner/expires/token 任一缺失（旧库遗留） | `lease_corrupt` |
| 3 | 当前持有者是**别人**（已被其他消费者接管） | `lease_held_by_other` |
| 4 | 同消费者名，但**凭证对不上**（同名新一轮已接管旧一轮） | `invalid_token` |
| 5 | 凭证正确但租约已过期（到期点本身算过期） | `lease_expired` |
| 6 | 租约有效但客户端 `expected_version` 过期 | `version_conflict` |
| — | 任务不存在 | `task_not_found`（HTTP 404） |

底层条件 UPDATE 也带 `lease_token` 与到期条件做二次兜底：即便判定后发生并发
轮换，`rowcount=0` 仍以 `invalid_token` 拒绝，绝不静默覆盖。

### 2.4 heartbeat / release 的结果枚举

- `heartbeat`：`renewed`（续期成功，version+1）/ `not_held`（非持有者或非运行中）/
  `invalid_token`（同消费者名的旧凭证）/ `expired`（租约已过期）/
  `corrupt`（租约字段缺失）。
- `release`：`released`（主动释放，立即回 PENDING 可重领）/ `not_held` /
  `invalid_token` / `already_expired`（已过期，不改写状态）/ `corrupt`。

## 3. 失败重试与退避

- 每次 `acquire` 令 `attempt += 1`。
- `fail()` 时：
  - `attempt < max_attempts`：回 PENDING，按指数退避设置
    `available_at = now + backoff`（默认 `backoff_base * 2**(attempt-1)`，
    也可显式传 `backoff_seconds`），退避窗口内不可领取。
  - `attempt >= max_attempts`：进入终态 FAILED，永不再被领取。
- `complete()` 进入终态 SUCCEEDED，清除租约并保存 `result`。

## 4. 异常 RUNNING 任务恢复（旧版本升级）

从旧版本升级的库里，可能有一直卡在 RUNNING、租约字段缺失（没有 owner/到期/
凭证）或租约早已过期的任务，它们既不会被领取扫描命中（缺字段的行不满足领取
条件），也无法提交结果。系统提供明确的三分类与恢复入口。

### 4.1 三分类：`classify_running(task_id)` / `GET /tasks/admin/stuck`

| 分类 (`RunningLeaseState`) | 判定 | 含义 |
| --- | --- | --- |
| `active` | owner、token、expires 齐全且 `expires > now` | 仍被有效持有，恢复**绝不触碰** |
| `expired` | 字段齐全但 `expires <= now` | 租约已过期，可安全置回可领取 |
| `corrupt` | owner/expires/token 任一缺失 | 旧库遗留的异常租约，需恢复 |

`list_stuck_running(limit)` 只返回 `expired` 与 `corrupt`，`active` 不会出现。

### 4.2 恢复动作：`recover_stuck(...)` / `POST /tasks/admin/recover`

- **expired** → 置回 `PENDING`、`available_at=now`，立即可以重新领取（`requeued`）。
- **corrupt** → 默认标记为 `QUARANTINED` 等待人工（`quarantined`）；
  传 `quarantine_corrupt=False`（CLI 的 `--auto-requeue`）则也直接重新可领取。
- **active** → 跳过（`skipped_active`），绝不改写。显式对单个 active 任务调用
  也只会得到跳过结论。
- 支持 `task_id`（只处理一条）、`batch_size`（分批）、`dry_run`（只预览不写入）。

恢复的安全性保证：

1. **幂等无副作用**：恢复条件用条件 UPDATE（只匹配“非有效持有”的行），
   重复执行不会反复改 version、不会重复记事件；没有候选时直接返回空。
2. **不误伤正常执行**：active 任务在 SELECT 阶段即跳过；条件 UPDATE 也只匹配
   过期或缺字段行，若任务恰好在判定与写入之间被续期/接管，`rowcount=0` 跳过。
3. **不挡正常领取/提交**：每批独立短事务提交，不持有长事务；恢复与正常业务
   共用同一套行级条件，互不阻塞。

隔离任务经人工确认后可用 `requeue_quarantined(task_id)`
（`POST /tasks/admin/recover/{id}/requeue`）放回可领取队列。

### 4.3 运维命令行

```bash
python -m tasklease.cli --db tasklease.db stuck                 # 查看卡死任务及分类
python -m tasklease.cli --db tasklease.db recover --dry-run     # 预览将要执行的动作
python -m tasklease.cli --db tasklease.db recover               # 过期->重领，异常->隔离
python -m tasklease.cli --db tasklease.db recover --auto-requeue  # 异常也自动重领
python -m tasklease.cli --db tasklease.db requeue <task_id>     # 隔离任务人工放行
python -m tasklease.cli --db tasklease.db archive --min-age-seconds 86400
```

## 5. 结果与执行历史查询

| 方法 | 说明 |
| --- | --- |
| `get_result(task_id)` | 读回最终结论：`status`、成功 `result`、失败 `last_error`、attempt、是否已归档 |
| `list_events(task_id, limit)` | 读回完整执行历史（领取/续期/释放/失败/完成/恢复/归档），含 actor 与 lease_token |
| `list_tasks(...)` | 主表任务分页筛选 |
| `list_archived_tasks(...)` | 归档表任务分页筛选 |

`list_tasks` / `list_archived_tasks` 的筛选与分页参数：

- `statuses`：状态列表（如 `[TaskStatus.FAILED, TaskStatus.SUCCEEDED]`）；
- `created_from / created_to`、`updated_from / updated_to`：创建/更新时间范围
  （闭区间，epoch 秒）；
- `limit`（默认 50）与 `cursor`（上一页返回的 `next_cursor`，首次不传）；
- 返回 `Page(items, limit, next_cursor)`，`next_cursor is None` 表示已到尾。
  游标基于底层 `rowid`，单调稳定，翻页不重不漏。

事件（`EventRecord`）字段：`id, task_id, event, actor, lease_token,
from_status, to_status, version, reason, created_at, archived`。归档后的历史
通过对主表与归档事件表 `UNION` 读回（按原事件序号排序），`archived=True`
标识该条来自归档表。

## 6. 终态任务归档

`archive_terminal(min_age_seconds=86400, batch_size=200, dry_run=False)`
（`POST /tasks/admin/archive`）把终结已久的 SUCCEEDED/FAILED 任务搬到
独立的 `tasks_archive` / `task_events_archive` 表：

- **归档范围**：仅终态且 `updated_at <= now - min_age_seconds` 的任务；
  非终态、刚终结的不动。
- **归档后仍可读**：`get`/`get_result` 默认回查归档表，最终状态、`result`、
  `last_error` 与完整 `list_events` 历史都能读回，并多一条 `archive` 事件。
- **既有语义不回归**：已归档终态的重复入队仍被 `task_terminal` 拒绝，
  `get_by_idempotency_key` 仍能命中归档任务。
- **不挡业务**：分批短事务，每批独立提交；归档期间新任务照常领取与提交。
- **幂等**：归档即从主表删除，重复执行返回 0、不重复记录归档事件。

兼容范围：v1/v2 的旧库由 `schema.initialize()` 平滑升到 v3（自动补
`lease_token`、`archived_at` 列与归档表）；旧行的 `lease_token` 为 NULL，
新领取时才发放凭证，因此旧数据可正常走完后续生命周期。

## 7. 并发约束与防覆盖

- **进程内**：所有会话经一把可重入锁串行化，连接设置 `busy_timeout`。
- **跨进程**：SQLite（WAL 模式）写事务天然串行化；领取/释放/续期/提交/恢复全部
  使用带状态、持有者、**凭证**、版本、租约到期条件的 UPDATE，冲突时 `rowcount=0`，
  不会静默覆盖。
- **乐观版本控制**：每行有单调递增的 `version`，每次状态变更 +1；
  `complete/fail` 可携带 `expected_version`，不符则 `version_conflict`。
- 所有关键流转写入 `task_events` 审计表（事件、操作者、**凭证**、from/to 状态、
  版本、原因），结论“可解释”。

## 8. HTTP 接口（与原服务共存）

挂载在 `/tasks` 前缀下，原有 `GET / -> {"Hello":"World"}` 保持不变：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /tasks` | 幂等入队，body：`idempotency_key, payload, max_attempts` |
| `POST /tasks/acquire` | 领取，body：`consumer, lease_seconds?`；返回 `lease_token` |
| `POST /tasks/{id}/complete` | 成功提交，body：`consumer, lease_token, result?, expected_version?` |
| `POST /tasks/{id}/fail` | 失败上报，body：`consumer, lease_token, error?, backoff_seconds?, expected_version?` |
| `POST /tasks/{id}/heartbeat` | 续期，body：`consumer, lease_token, lease_seconds?` |
| `POST /tasks/{id}/release` | 主动释放，body：`consumer, lease_token` |
| `GET  /tasks/{id}` | 查询任务（默认含归档回查，`?include_archive=false` 关闭） |
| `GET  /tasks/{id}/result` | 读回最终状态/产出/失败原因 |
| `GET  /tasks/{id}/events?limit=` | 读回执行历史（含归档） |
| `GET  /tasks?status=&created_from=&created_to=&updated_from=&updated_to=&limit=&cursor=` | 分页筛选主表任务 |
| `GET  /tasks/archive/list?...` | 分页筛选归档任务（参数同上） |
| `GET  /tasks/admin/stuck` | 列出 expired/corrupt 的 RUNNING 及分类 |
| `POST /tasks/admin/recover?dry_run=&batch_size=&quarantine_corrupt=&task_id=` | 恢复异常 RUNNING |
| `POST /tasks/admin/recover/{id}/requeue` | 人工放行隔离任务 |
| `POST /tasks/admin/archive?min_age_seconds=&batch_size=&dry_run=` | 归档终态任务 |

非法操作返回 `409`（任务不存在为 `404`，缺凭证为 `422`），响应形如：

```json
{"detail": {"reason": "invalid_token",
            "message": "消费者 ... 出示的执行凭证已失效：任务 ... 的租约已轮换给同消费者名的新一轮执行，旧一轮的提交不得覆盖新产出"}}
```

## 9. 本地验证

```bash
# 安装依赖（uv）
uv sync

# 运行全部单测
uv run pytest -q
uv run pytest -s -o log_cli=true          # 查看详细判定日志

# 启动 HTTP 服务并手动验证
uv run uvicorn main:app --port 8000
curl -s localhost:8000/                                   # 原有行为不变
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","payload":{"n":1}}'
curl -s -X POST localhost:8000/tasks/acquire -H 'Content-Type: application/json' \
  -d '{"consumer":"c1","lease_seconds":10}'
# -> {"acquired":true,"lease_token":"<token>","task":{...}}
# 后续 complete/fail/heartbeat/release 都带上 lease_token
```

编程式用法（凭证 + 历史读回）：

```python
from tasklease import TaskStore, TaskRejected, RejectReason

store = TaskStore("tasklease.db", default_lease_seconds=30)
task, _ = store.enqueue("order-123", {"url": "..."}, max_attempts=3)
claimed = store.acquire("worker-A")
if claimed:
    job, token = claimed
    try:
        store.complete(job.id, "worker-A", token, {"output": "..."},
                       expected_version=job.version)
    except TaskRejected as exc:
        if exc.reason is RejectReason.LEASE_EXPIRED:
            ...          # 租约已过期，交由重新领取
        elif exc.reason is RejectReason.INVALID_TOKEN:
            ...          # 同名将任务已被新一轮接管，本次产出必须放弃
        elif exc.reason is RejectReason.LEASE_HELD_BY_OTHER:
            ...          # 已被别的消费者接管

# 读回最终产出与逐次尝试历史
print(store.get_result(task.id))
for e in store.list_events(task.id):
    print(e.created_at, e.event, e.actor, e.lease_token, e.reason)
```

### 测试覆盖重点（tests/）

| 文件 | 覆盖点 |
| --- | --- |
| `test_execution_token.py` | 每次领取凭证轮换；轮换瞬间旧凭证四类操作全失效；同名消费者旧提交无法覆盖新产出；过期/被接管/凭证不符三类结论分开；伪造凭证拒绝；事件携带凭证可审计 |
| `test_recovery.py` | active/expired/corrupt 三分类；卡死扫描排除 active；corrupt 隔离、expired 重领；dry-run 无副作用；恢复幂等；active 绝不被改写；恢复不挡正常领取提交；隔离后可重走生命周期 |
| `test_history_archive.py` | 成功产出/失败原因/逐次尝试读回；分页不重不漏与状态/时间筛选；归档后状态产出历史可读；归档后重复入队仍 `task_terminal`；归档排除非终态与新终态、分批、幂等、不挡新任务 |
| `test_http_api.py` | 上述能力经 HTTP 端到端验证，含 409/404/422 可区分结论 |
| `test_lease.py / test_enqueue.py / test_retry_version.py / test_concurrency.py` | 原有领取、租约、幂等入队、重试、乐观版本、并发不双执行等用例（已适配凭证 API） |
