#!/usr/bin/env python3
"""
Планировщик smart-call.
Запускается раз в минуту через cron.
Читает очередь из PostgreSQL, определяет кого звонить,
запускает dispatch_outbound.py через docker exec.

НЕ использует docker.sock внутри контейнера.
Работает на хосте под ubuntu.

Правила:
1. Проверка окна звонка — ВСЕГДА первая.
2. Если окно закрыто — статус stop, next_callback_at обнуляется.
3. next_callback_at учитывается ТОЛЬКО если окно открыто.
4. Зависшие попытки (> 10 минут) сбрасываются автоматически.
5. Не звонить, если есть активная попытка (< 5 минут).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg

# === Конфигурация ===
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "voice"
DB_USER = "voice_migrator"
DB_PASSWORD_FILE = Path("/data/voice-assistant/integration/secrets/postgres-migrator-password")

AGENT_CONTAINER = "voice-ai-agent-1"
DISPATCH_SCRIPT = "/app/scripts/dispatch_outbound.py"
SCENARIO_ID = "vtd_order"
PURPOSE = "smart_call"

# Логика
CALL_BEFORE_DEADLINE_MIN = 60        # начинаем за 1 час до дедлайна
CALLBACK_INTERVAL_MIN = 15           # интервал между попытками
MAX_ATTEMPTS = 4                     # максимум попыток на клиента
STOP_BEFORE_DEADLINE_MIN = 5         # не звоним за 5 минут до дедлайна
MIN_CALLBACK_MINUTES = 5             # минимум 5 минут для перезвона

# Защита от зависаний
STALE_ATTEMPT_MINUTES = 10           # если попытка висит > 10 мин — сбрасываем
ACTIVE_ATTEMPT_MINUTES = 5           # не звоним, если попытка < 5 мин назад

# Логи
LOG_FILE = Path("/home/ubuntu/smart-call-tick.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("smart-call-tick")


def load_db_password() -> str:
    if not DB_PASSWORD_FILE.exists():
        raise RuntimeError(f"password file not found: {DB_PASSWORD_FILE}")
    return DB_PASSWORD_FILE.read_text().strip()


async def get_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=load_db_password(),
        min_size=1,
        max_size=2,
    )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def deadline_today(h: int, m: int, tz_offset_hours: int = 3) -> datetime:
    """
    Возвращает datetime дедлайна для сегодняшнего дня в UTC.
    tz_offset_hours — смещение локального времени от UTC (МСК = +3).
    """
    now_local = now_utc() + timedelta(hours=tz_offset_hours)
    deadline_local = now_local.replace(hour=h, minute=m, second=0, microsecond=0)
    return deadline_local - timedelta(hours=tz_offset_hours)


async def reset_stale_attempts(pool: asyncpg.Pool) -> int:
    """Сбрасывает попытки, которые висят > STALE_ATTEMPT_MINUTES."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            f"""
            UPDATE voice_integration.smart_call_attempts
            SET finished_at = NOW(),
                outcome = 'ЗАВИС',
                reason = 'Попытка не завершилась за {STALE_ATTEMPT_MINUTES} минут'
            WHERE finished_at IS NULL
              AND started_at < NOW() - INTERVAL '{STALE_ATTEMPT_MINUTES} minutes'
            """
        )
        try:
            count = int(result.split()[-1])
        except (ValueError, IndexError):
            count = 0
        if count > 0:
            log.warning("Сброшено зависших попыток: %d", count)
        return count


async def find_clients_to_call(pool: asyncpg.Pool) -> list[dict]:
    """
    Возвращает список клиентов, которых нужно звонить сейчас.

    Логика (по приоритету):
    1. Окно закрыто → stop (не звоним).
    2. Ещё рано → пропускаем.
    3. next_callback_at в будущем → пропускаем.
    4. Всё ок → звоним.
    """
    now = now_utc()
    log.info("Tick at %s", now.isoformat())

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                c.id,
                c.task_id,
                c.name,
                c.phone,
                c.deadline_h,
                c.deadline_m,
                c.status,
                c.last_outcome,
                c.next_callback_at,
                c.attempts
            FROM voice_integration.smart_call_clients c
            JOIN voice_integration.smart_call_tasks t ON t.id = c.task_id
            WHERE t.status = 'active'
              AND c.status IN ('pending', 'calling')
              AND c.attempts < $1
              AND NOT EXISTS (
                  SELECT 1 FROM voice_integration.smart_call_attempts a
                  WHERE a.client_id = c.id
                    AND a.finished_at IS NULL
                    AND a.started_at > NOW() - ($2::text || ' minutes')::interval
              )
            ORDER BY c.deadline_h, c.deadline_m, c.id
            LIMIT 20
            """,
            MAX_ATTEMPTS,
            str(ACTIVE_ATTEMPT_MINUTES),
        )

    to_call: list[dict] = []

    for r in rows:
        client = dict(r)

        # === 1. ВСЕГДА считаем окно ===
        deadline = deadline_today(client["deadline_h"], client["deadline_m"])
        window_start = deadline - timedelta(minutes=CALL_BEFORE_DEADLINE_MIN)
        window_end = deadline - timedelta(minutes=STOP_BEFORE_DEADLINE_MIN)

        # === 2. Окно закрыто → stop ===
        if now > window_end:
            log.info(
                "Клиент %d (%s): окно закрыто (дедлайн %s UTC), стоп",
                client["id"], client["phone"], deadline.isoformat(),
            )
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE voice_integration.smart_call_clients
                    SET status = 'stop',
                        last_outcome = 'ОКНО_ЗАКРЫТО',
                        next_callback_at = NULL,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    client["id"],
                )
            continue

        # === 3. Ещё рано → пропускаем ===
        if now < window_start:
            continue

        # === 4. next_callback_at в будущем → пропускаем ===
        if client["next_callback_at"] and now < client["next_callback_at"]:
            continue

        # === 5. Всё ок — звоним ===
        to_call.append(client)

    return to_call


def build_session_id(client_id: int, attempt: int) -> str:
    """Формирует session_id для звонка."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"smart-call-{ts}-{client_id}-{attempt}"


def dispatch_call(phone: str, session_id: str) -> dict | None:
    """Запускает dispatch_outbound.py внутри контейнера агента."""
    cmd = [
        "sudo", "docker", "exec", AGENT_CONTAINER,
        "python", DISPATCH_SCRIPT,
        "--callee", phone,
        "--purpose", PURPOSE,
        "--scenario-id", SCENARIO_ID,
        "--session-id", session_id,
    ]
    log.info("Dispatch: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            log.error("Dispatch failed: %s", result.stderr[:500])
            return None
        try:
            return json.loads(result.stdout.strip().split("\n")[-1])
        except json.JSONDecodeError:
            log.error("Dispatch stdout not JSON: %s", result.stdout[:500])
            return None
    except subprocess.TimeoutExpired:
        log.error("Dispatch timeout")
        return None
    except Exception as e:
        log.exception("Dispatch error: %s", e)
        return None


async def start_call(pool: asyncpg.Pool, client: dict) -> None:
    """Запускает звонок клиенту."""
    attempt = client["attempts"] + 1
    session_id = build_session_id(client["id"], attempt)

    log.info(
        "Звоним #%d (%s), попытка %d, session=%s",
        client["id"], client["phone"], attempt, session_id,
    )

    # Помечаем "calling" заранее
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE voice_integration.smart_call_clients
            SET status = 'calling',
                attempts = attempts + 1,
                last_session_id = $2,
                updated_at = NOW()
            WHERE id = $1
            """,
            client["id"], session_id,
        )

    dispatch = dispatch_call(client["phone"], session_id)
    if not dispatch:
        log.error("Не удалось запустить звонок для #%d", client["id"])
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE voice_integration.smart_call_clients
                SET status = 'pending',
                    last_outcome = 'ОШИБКА_ЗАПУСКА',
                    next_callback_at = NOW() + INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = $1
                """,
                client["id"],
            )
        return

    # Логируем попытку
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO voice_integration.smart_call_attempts
                (client_id, session_id, started_at)
            VALUES ($1, $2, NOW())
            """,
            client["id"], session_id,
        )

    log.info("Звонок #%d запущен: %s", client["id"], dispatch.get("dispatch_id"))


async def main() -> None:
    try:
        pool = await get_pool()
    except Exception as e:
        log.exception("Не удалось подключиться к PostgreSQL: %s", e)
        sys.exit(1)

    try:
        # 1. Сначала сбрасываем зависшие попытки
        await reset_stale_attempts(pool)

        # 2. Потом ищем клиентов для звонка
        to_call = await find_clients_to_call(pool)
        log.info("Клиентов к обзвону: %d", len(to_call))
        for client in to_call:
            await start_call(pool, client)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
