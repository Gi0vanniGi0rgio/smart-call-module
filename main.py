from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .db import ensure_tables, get_pool
from .parser import parse_client_list

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("smart-call")

PORT = int(os.getenv("SMART_CALL_PORT", "8030"))

pool: asyncpg.Pool | None = None
templates = Jinja2Templates(directory="/app/app/templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await get_pool()
    await ensure_tables(pool)
    log.info("smart-call started on port %d", PORT)
    yield
    await pool.close()


app = FastAPI(lifespan=lifespan, docs_url="/smart-call/api/docs")


class ParseRequest(BaseModel):
    text: str = Field(min_length=1, max_length=1_000_000)


class CreateTaskRequest(BaseModel):
    text: str = Field(min_length=1, max_length=1_000_000)
    notes: str = Field(default="", max_length=500)


# === Парсинг списка ===
@app.post("/smart-call/api/parse")
async def parse_list(req: ParseRequest):
    clients, errors, total = parse_client_list(req.text)
    return {
        "clients": clients,
        "errors": errors,
        "total": total,
        "valid": len(clients),
    }


# === Создание задачи ===
@app.post("/smart-call/api/tasks")
async def create_task(req: CreateTaskRequest):
    clients, errors, total = parse_client_list(req.text)
    if not clients:
        raise HTTPException(status_code=400, detail={
            "message": "Нет валидных клиентов",
            "errors": errors[:10],
        })

    async with pool.acquire() as conn:
        async with conn.transaction():
            task_id = await conn.fetchval(
                """
                INSERT INTO voice_integration.smart_call_tasks
                    (status, notes, total_clients)
                VALUES ('active', $1, $2)
                RETURNING id
                """,
                req.notes or None,
                len(clients),
            )

            for c in clients:
                await conn.execute(
                    """
                    INSERT INTO voice_integration.smart_call_clients
                        (task_id, name, phone, deadline_h, deadline_m, status)
                    VALUES ($1, $2, $3, $4, $5, 'pending')
                    ON CONFLICT (task_id, phone) DO NOTHING
                    """,
                    task_id, c["name"], c["phone"], c["deadline_h"], c["deadline_m"],
                )

    log.info("Создан task %d с %d клиентами", task_id, len(clients))
    return {
        "task_id": task_id,
        "clients_added": len(clients),
        "errors": errors[:10],
    }


# === Список задач ===
@app.get("/smart-call/api/tasks")
async def list_tasks():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT t.id, t.created_at, t.status, t.total_clients, t.notes,
                   COUNT(c.id) FILTER (WHERE c.status = 'pending') AS pending,
                   COUNT(c.id) FILTER (WHERE c.status = 'calling') AS calling,
                   COUNT(c.id) FILTER (WHERE c.status = 'done') AS done,
                   COUNT(c.id) FILTER (WHERE c.status = 'stop') AS stop
            FROM voice_integration.smart_call_tasks t
            LEFT JOIN voice_integration.smart_call_clients c ON c.task_id = t.id
            GROUP BY t.id
            ORDER BY t.created_at DESC
            LIMIT 50
            """
        )
    return {"tasks": [dict(r) for r in rows]}


# === Список клиентов задачи (с last_reason) ===
@app.get("/smart-call/api/tasks/{task_id}/clients")
async def list_clients(task_id: int):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                c.id, c.name, c.phone, c.deadline_h, c.deadline_m,
                c.status, c.last_outcome, c.next_callback_at, c.attempts,
                c.backend_session_id, c.last_session_id, c.updated_at,
                a.reason AS last_reason,
                a.callback_minutes AS last_callback_minutes,
                a.outcome AS last_attempt_outcome
            FROM voice_integration.smart_call_clients c
            LEFT JOIN voice_integration.smart_call_attempts a
              ON a.client_id = c.id
             AND a.id = (
                 SELECT MAX(id) FROM voice_integration.smart_call_attempts
                 WHERE client_id = c.id
             )
            WHERE c.task_id = $1
            ORDER BY c.deadline_h, c.deadline_m, c.id
            """,
            task_id,
        )
    return {"clients": [dict(r) for r in rows]}


# === История попыток клиента ===
@app.get("/smart-call/api/clients/{client_id}/attempts")
async def list_attempts(client_id: int):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, session_id, started_at, finished_at,
                   outcome, callback_minutes, reason
            FROM voice_integration.smart_call_attempts
            WHERE client_id = $1
            ORDER BY started_at DESC
            LIMIT 20
            """,
            client_id,
        )
    return {"attempts": [dict(r) for r in rows]}


# === HTML-страница ===
@app.get("/smart-call", response_class=HTMLResponse)
async def page(request: Request):
    return templates.TemplateResponse("smart.html", {"request": request})


# === Health ===
@app.get("/smart-call/health")
async def health():
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok"}
