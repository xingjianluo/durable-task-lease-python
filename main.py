"""FastAPI 入口：原有 "/" 行为不变，新增 "/tasks" 任务领取/租约接口。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from tasklease.api import create_router
from tasklease.store import TaskStore

DB_PATH = os.environ.get("TASKLEASE_DB", "tasklease.db")

# 模块级单例：无外部服务依赖，数据落在本地 SQLite 文件。
store = TaskStore(DB_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        yield
    finally:
        store.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "World"}


app.include_router(create_router(store))
