# durable-task-lease-python

本地任务领取/执行持久化存储层：**无外部服务依赖**（仅用内置 SQLite），
支持幂等入队、状态机约束、并发领取、租约管理、失败退避重试与乐观并发控制。

原有 FastAPI 服务行为保持不变：`GET /` 仍返回 `{"Hello": "World"}`；
新增能力统一挂在 `/api` 前缀。

## 目录结构

```
main.py                  # FastAPI 入口：原根路由 + 挂载 /api 路由
task_store/
  types.py               # 状态机、拒绝原因、租约/结果类型
  clock.py               # SystemClock / FakeClock
  schema.py              # SQLite schema 与 v0→v1 幂等迁移
  store.py               # TaskStore：条件 UPDATE 实现的原子状态流转
  service.py             # 带判定日志的门面服务
  api/                   # HTTP 路由与 pydantic 模型
tests/                   # 并发领取/租约过期/重复入队/非法流转/升级兼容
docs/design.md           # 状态机、租约、重试、并发约束详解
```

## 本地验证

```bash
# 1. 安装依赖（需要 Python >= 3.14.4）
uv sync

# 2. 运行全部单测（-s 可在日志看到入队标识、租约信息、状态流转判定依据）
uv run pytest -s

# 3. 启动服务
uv run fastapi dev main.py
# 或：uv run uvicorn main:app --reload
```

启动后：

- 原接口：`curl http://127.0.0.1:8000/` → `{"Hello":"World"}`
- 交互式文档：<http://127.0.0.1:8000/docs>
- 数据库文件：当前目录 `tasks.db`（SQLite，WAL 模式）。

## API 速览

所有“提交类”接口的 body 都要带 `owner` 与领取时获得的 `fencing_token`。

```bash
# 幂等入队
curl -X POST localhost:8000/api/tasks \
  -H 'content-type: application/json' \
  -d '{"idempotency_key":"order-42","payload":"charge","max_attempts":3}'
# → {"outcome":"CREATED",...}；重复提交同样的 key → "DEDUPED_PENDING"
# 正在执行 → 409 reason=ALREADY_RUNNING；终态 → 409 reason=ALREADY_TERMINAL

# 领取（返回租约；无任务时 outcome=EMPTY）
curl -X POST localhost:8000/api/tasks/claim \
  -H 'content-type: application/json' -d '{"owner":"worker-1","lease_seconds":30}'

# 续期 / 主动释放
curl -X POST localhost:8000/api/tasks/<id>/renew   -d '{"owner":"worker-1","fencing_token":1,"lease_seconds":60}' -H 'content-type: application/json'
curl -X POST localhost:8000/api/tasks/<id>/release -d '{"owner":"worker-1","fencing_token":1}' -H 'content-type: application/json'

# 提交成功 / 失败（过期租约→LEASE_EXPIRED；旧 token→HELD_BY_OTHER）
curl -X POST localhost:8000/api/tasks/<id>/complete -d '{"owner":"worker-1","fencing_token":1,"result_payload":"ok"}' -H 'content-type: application/json'
curl -X POST localhost:8000/api/tasks/<id>/fail     -d '{"owner":"worker-1","fencing_token":1,"error_message":"timeout"}' -H 'content-type: application/json'

# 查询
curl localhost:8000/api/tasks
curl localhost:8000/api/tasks/<id>
```

冲突响应体（HTTP 409）形如：

```json
{"detail": {"reason": "ALREADY_RUNNING", "message": "...", "task_id": "..."}}
```

`reason` 取值即 `docs/design.md` 第 2 节的可区分拒绝原因；版本冲突为
`VERSION_CONFLICT`，任务不存在为 404 `NOT_FOUND`。

## 代码中直接使用

```python
from task_store.service import DurableTaskService

with DurableTaskService("tasks.db", lease_seconds=30) as svc:
    svc.enqueue("order-42", payload="charge")
    outcome, lease = svc.claim("worker-1")
    if lease:
        try:
            # ... 执行任务 ...
            svc.complete(lease.task_id, lease.owner, lease.fencing_token, "done")
        except Exception as exc:  # noqa
            svc.fail(lease.task_id, lease.owner, lease.fencing_token, str(exc))
```

更完整的状态机、租约与重试规则、并发约束、升级策略见 [docs/design.md](docs/design.md)。
