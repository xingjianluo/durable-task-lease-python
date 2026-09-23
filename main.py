from fastapi import FastAPI

from task_store.api.routes import create_router
from task_store.service import DurableTaskService

app = FastAPI()

# 本地持久化任务服务（SQLite，无外部依赖）。原有对外行为保持不变，
# 新能力统一挂载在 /api 前缀下。
_task_service = DurableTaskService(db_path="tasks.db")
app.include_router(create_router(_task_service))


@app.get("/")
def read_root():
    return {"Hello": "World"}
