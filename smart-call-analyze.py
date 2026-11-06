#!/usr/bin/env python3
"""
Анализатор диалогов smart-call.
Запускается раз в минуту через cron.
Читает завершённые звонки smart-call из call_sessions,
отправляет транскрипт в RouterAI, обновляет smart_call_clients.

Особенности:
- Детектор автоответчика (по паттернам) — до вызова LLM.
- Категория АВТООТВЕТЧИК в промпте LLM.
- Постобработка (refusal/order) для надёжности.
- АВТООТВЕТЧИК → перезвон через 30 минут.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import httpx

# === Конфигурация ===
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "voice"
DB_USER = "voice_migrator"
DB_PASSWORD_FILE = Path("/data/voice-assistant/integration/secrets/postgres-migrator-password")

ROUTERAI_URL = os.getenv("ROUTERAI_URL", "https://routerai.ru/api/v1")
ROUTERAI_KEY = os.getenv("ROUTERAI_KEY", "sk-5Ts7Gx2VUXCxPXhaO8lyIgeiuL-C0fUE")
LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-5.4-mini")
LLM_TIMEOUT = 60

MAX_ATTEMPTS = 4
CALLBACK_INTERVAL_MIN = 15
MIN_CALLBACK_MINUTES = 5
MAX_CALLBACK_MINUTES = 1440
STOP_BEFORE_DEADLINE_MIN = 5

# Категории, после которых звонки прекращаются
STOP_OUTCOMES = (
    "ЗАКАЗ_ОФОРМЛЕН",
    "ЗАКАЗ_УЖЕ_ЕСТЬ",
    "ОТКАЗ_СЕГОДНЯ",
    "НЕЦЕЛЕВОЙ",
    "ТРЕБУЕТ_ОПЕРАТОРА",
    "ПЕРЕЗВОНИТЬ_ЗАВТРА",
)

# Категории, после которых перезваниваем
CALLBACK_OUTCOMES = (
    "ПЕРЕЗВОНИТЬ_ПОЗЖЕ",
    "ЗАНЯТ",
    "НЕ_ДОЗВОНИЛИСЬ",
    "АВТООТВЕТЧИК",
)

# === Паттерны автоответчика ===
AUTORESPONDER_PATTERNS = [
    r"я все передам",
    r"я всё передам",
    r"передам абоненту",
    r"передам это абоненту",
    r"передам информацию абоненту",
    r"оставьте сообщение",
    r"оставьте ваше сообщение",
    r"оставьте свое сообщение",
    r"абонент не отвечает",
    r"абонент недоступен",
    r"абонент сейчас не может",
    r"перезвоните позже",
    r"после сигнала",
    r"после звукового сигнала",
    r"ваш звонок очень важен",
    r"в какое время можно с вами связаться",
    r"в какое время с вами можно связаться",
    r"подскажите я с человеком разговариваю",
    r"я с человеком разговариваю",
    r"может быть заодно расскажете",
    r"что то желаете добавить я все передам",
    r"что-то желаете добавить",
    r"голосовое сообщение",
    r"автоответчик",
]

# === Ключевые слова для постобработки ===
REFUSAL_KEYWORDS = [
    "не надо", "не сегодня", "не звоните", "пропустим", "пропускаем",
    "в отпуске", "заказа не будет", "нам не нужно",
]

PRODUCT_KEYWORDS = [
    "бедро", "бедра", "крыл", "окороч", "филе", "грудк", "тушк",
    "голен", "фарш", "рулет", "шашлык", "печен", "сердц", "желудк",
    "утк", "утен", "индейк", "куриц", "кур", "цыплен", "паштет",
    "колбас", "сосиск", "яйц", "полутуш", "четвертин", "спинк",
    "суповой", "набор", "коробк", "упаковк", "ящик", "пакет",
    "кг", "килограмм", "штук", "шт",
]

LOG_FILE = Path("/home/ubuntu/smart-call-analyze.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("smart-call-analyze")


SYSTEM_PROMPT = """Ты анализируешь транскрипт телефонного звонка между голосовым агентом (компания ВТД, менеджер Анна) и клиентом (оптовый покупатель продуктов).

В транскрипте роли размечены:
- "Агент:" — говорит наш голосовой агент (Анна).
- "Клиент:" — говорит клиент (или автоответчик на стороне клиента).

КАТЕГОРИИ (выбери одну):

1. ЗАКАЗ_ОФОРМЛЕН — клиент сделал заказ, назвал хотя бы один товар.
2. ЗАКАЗ_УЖЕ_ЕСТЬ — клиент сказал, что заказ УЖЕ сделан ранее.
3. ОТКАЗ_СЕГОДНЯ — клиент ЯВНО отказался сегодня.
4. ПЕРЕЗВОНИТЬ_ПОЗЖЕ — клиент попросил перезвонить СЕГОДНЯ (через N минут).
5. ПЕРЕЗВОНИТЬ_ЗАВТРА — клиент попросил перезвонить НЕ СЕГОДНЯ (завтра, в понедельник, на следующей неделе).
6. НЕ_ДОЗВОНИЛИСЬ — не взял трубку, сброс, тишина, нет реплик клиента.
7. ЗАНЯТ — "занят", "перезвоните позже" (в пределах сегодня).
8. НЕЦЕЛЕВОЙ — ошиблись номером.
9. ТРЕБУЕТ_ОПЕРАТОРА — ЖИВОЙ клиент просит переключить на человека ("дайте оператора", "не хочу с роботом", "соедините с менеджером").
10. АВТООТВЕТЧИК — на стороне клиента автоответчик или голосовой робот.

ПРИЗНАКИ АВТООТВЕТЧИКА (ставь АВТООТВЕТЧИК, если есть хотя бы 2):
- "я все передам", "передам абоненту", "передам это абоненту"
- "оставьте сообщение", "после сигнала"
- "абонент недоступен", "абонент не отвечает"
- "в какое время можно с вами связаться"
- "я с человеком разговариваю", "подскажите я с человеком разговариваю"
- "может быть заодно расскажете"
- монотонный голос, странные не по контексту вопросы

КРИТИЧНО:
- ТРЕБУЕТ_ОПЕРАТОРА — только если ЖИВОЙ клиент явно просит человека.
- АВТООТВЕТЧИК — если фразы похожи на робота/автоответчик.
- НЕ путай: "я передам абоненту" — это АВТООТВЕТЧИК, НЕ ТРЕБУЕТ_ОПЕРАТОРА.
- Если клиент назвал товар — ЗАКАЗ_ОФОРМЛЕН (наивысший приоритет после АВТООТВЕТЧИК).

ВЫВОД: строго JSON, без markdown:
{
  "outcome": "КАТЕГОРИЯ",
  "delay_minutes": <целое число или null>,
  "reason": "<краткое объяснение на русском>"
}
"""


def load_db_password() -> str:
    if not DB_PASSWORD_FILE.exists():
        raise RuntimeError(f"password file not found: {DB_PASSWORD_FILE}")
    return DB_PASSWORD_FILE.read_text().strip()


async def get_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=load_db_password(),
        min_size=1, max_size=2,
    )


def is_autoresponder(transcript_text: str) -> tuple[bool, list[str]]:
    """Проверяет, похож ли транскрипт на автоответчик. Возвращает (bool, matches)."""
    t = (transcript_text or "").lower()
    matches = [p for p in AUTORESPONDER_PATTERNS if re.search(p, t)]
    # Порог: минимум 2 паттерна
    return (len(matches) >= 2, matches)


def has_refusal_keyword(transcript: str) -> list[str]:
    t = (transcript or "").lower()
    return [kw for kw in REFUSAL_KEYWORDS if kw in t]


def has_product_keywords(transcript: str) -> list[str]:
    t = (transcript or "").lower()
    client_text = " ".join(
        line[len("Клиент:"):].strip() for line in t.split("\n")
        if line.lower().startswith("клиент:")
    )
    return [kw for kw in PRODUCT_KEYWORDS if kw in client_text]


def validate_outcome(decision: dict, transcript: str) -> dict:
    """
    Постобработка решения LLM.
    Не трогаем АВТООТВЕТЧИК, ЗАКАЗ_ОФОРМЛЕН, ОТКАЗ_СЕГОДНЯ.
    """
    outcome = decision.get("outcome")

    # АВТООТВЕТЧИК не корректируем — это финально
    if outcome == "АВТООТВЕТЧИК":
        return decision

    # Если LLM сказал ПЕРЕЗВОНИТЬ_ПОЗЖЕ, но есть отказ — меняем на ОТКАЗ
    if outcome == "ПЕРЕЗВОНИТЬ_ПОЗЖЕ":
        matched = has_refusal_keyword(transcript)
        if matched:
            log.warning("Автокоррекция: ПЕРЕЗВОНИТЬ_ПОЗЖЕ → ОТКАЗ_СЕГОДНЯ (найдено %s)", matched)
            return {"outcome": "ОТКАЗ_СЕГОДНЯ", "delay_minutes": None,
                    "reason": f"Автокоррекция: найдено {matched}"}

    # Если LLM сказал ПЕРЕЗВОНИТЬ_ПОЗЖЕ/НЕ_ДОЗВОНИЛИСЬ, но клиент назвал товары — это заказ
    if outcome in ("ПЕРЕЗВОНИТЬ_ПОЗЖЕ", "НЕ_ДОЗВОНИЛИСЬ"):
        matched = has_product_keywords(transcript)
        if len(matched) >= 2:
            log.warning("Автокоррекция: %s → ЗАКАЗ_ОФОРМЛЕН (найдено %s)", outcome, matched[:5])
            return {"outcome": "ЗАКАЗ_ОФОРМЛЕН", "delay_minutes": None,
                    "reason": f"Автокоррекция: клиент назвал товары ({matched[:3]})"}

    return decision


def format_transcript(transcript_data) -> str:
    """Форматирует транскрипт в читаемый текст."""
    if isinstance(transcript_data, str):
        transcript_data = json.loads(transcript_data)

    lines = []
    for item in transcript_data:
        speaker = item.get("speaker", "unknown")
        text = (item.get("text") or "").strip()
        if not text:
            continue
        role = "Агент" if speaker == "assistant" else "Клиент"
        lines.append(f"{role}: {text}")

    return "\n".join(lines) if lines else "(пустой диалог)"


async def analyze_transcript(transcript_text: str) -> dict:
    """Отправляет транскрипт в RouterAI и получает решение."""
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Транскрипт звонка:\n\n{transcript_text}"},
        ],
        "temperature": 0.1,
        "max_tokens": 300,
    }
    headers = {
        "Authorization": f"Bearer {ROUTERAI_KEY}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        r = await client.post(f"{ROUTERAI_URL}/chat/completions",
                              headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()

    content = data["choices"][0]["message"]["content"]
    match = re.search(r'\{.*\}', content, re.DOTALL)
    if match:
        return json.loads(match.group())
    return json.loads(content)


async def process_completed_calls(pool: asyncpg.Pool) -> int:
    """Находит завершённые звонки smart-call без анализа и обрабатывает."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                cs.session_id,
                cs.transcript,
                cs.finished_at,
                c.id AS client_id,
                c.attempts,
                c.deadline_h,
                c.deadline_m,
                a.id AS attempt_id
            FROM voice_integration.call_sessions cs
            JOIN voice_integration.smart_call_clients c
              ON c.last_session_id = cs.session_id
            LEFT JOIN voice_integration.smart_call_attempts a
              ON a.client_id = c.id AND a.session_id = cs.session_id
            WHERE cs.session_id LIKE 'smart-call-%'
              AND cs.state = 'completed'
              AND cs.transcript IS NOT NULL
              AND (a.id IS NULL OR a.finished_at IS NULL)
            ORDER BY cs.finished_at DESC
            LIMIT 10
            """
        )

    if not rows:
        return 0

    log.info("Найдено завершённых звонков для анализа: %d", len(rows))

    processed = 0
    for row in rows:
        session_id = row["session_id"]
        client_id = row["client_id"]
        attempt_id = row["attempt_id"]

        try:
            transcript_text = format_transcript(row["transcript"])

            # 1. Пустой транскрипт
            if len(transcript_text.strip()) < 5:
                decision = {
                    "outcome": "НЕ_ДОЗВОНИЛИСЬ",
                    "delay_minutes": None,
                    "reason": "пустой транскрипт",
                }
                log.info("session=%s: пустой транскрипт → НЕ_ДОЗВОНИЛИСЬ", session_id)

            # 2. Автоответчик — до LLM
            else:
                is_ar, ar_matches = is_autoresponder(transcript_text)
                if is_ar:
                    decision = {
                        "outcome": "АВТООТВЕТЧИК",
                        "delay_minutes": 30,
                        "reason": f"Автоответчик (паттерны: {ar_matches[:3]})",
                    }
                    log.info("session=%s: автоответчик → АВТООТВЕТЧИК (перезавон через 30 мин)", session_id)
                else:
                    # 3. LLM
                    decision = await analyze_transcript(transcript_text)
                    decision = validate_outcome(decision, transcript_text)
        except Exception as e:
            log.exception("Ошибка анализа session=%s: %s", session_id, e)
            continue

        outcome = decision.get("outcome")
        delay_minutes = decision.get("delay_minutes")
        reason = decision.get("reason", "")

        # Нормализация delay_minutes
        if delay_minutes is not None:
            try:
                delay_minutes = max(
                    MIN_CALLBACK_MINUTES,
                    min(int(delay_minutes), MAX_CALLBACK_MINUTES),
                )
            except (TypeError, ValueError):
                delay_minutes = None

        # Определяем новый статус
        if outcome in STOP_OUTCOMES:
            new_status = "done"
            next_callback_at = None
        elif outcome in CALLBACK_OUTCOMES:
            new_status = "pending"
            delay = delay_minutes or CALLBACK_INTERVAL_MIN
            next_callback_at = datetime.now(timezone.utc) + timedelta(minutes=delay)
        else:
            # Неизвестный outcome — стоп, чтобы не звонить бесконечно
            new_status = "done"
            next_callback_at = None
            reason = f"Неизвестный outcome '{outcome}'. {reason}"

        # Проверка на исчерпание попыток
        if row["attempts"] >= MAX_ATTEMPTS:
            new_status = "done"
            next_callback_at = None
            reason = f"Исчерпаны попытки ({row['attempts']}). {reason}"

        async with pool.acquire() as conn:
            async with conn.transaction():
                if attempt_id is not None:
                    await conn.execute(
                        """
                        UPDATE voice_integration.smart_call_attempts
                        SET finished_at = $2,
                            outcome = $3,
                            callback_minutes = $4,
                            reason = $5,
                            llm_raw = $6::jsonb
                        WHERE id = $1
                        """,
                        attempt_id,
                        row["finished_at"],
                        outcome,
                        delay_minutes,
                        reason,
                        json.dumps(decision, ensure_ascii=False),
                    )
                else:
                    await conn.execute(
                        """
                        INSERT INTO voice_integration.smart_call_attempts
                            (client_id, session_id, started_at, finished_at,
                             outcome, callback_minutes, reason, llm_raw)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                        """,
                        client_id,
                        session_id,
                        row["finished_at"],
                        row["finished_at"],
                        outcome,
                        delay_minutes,
                        reason,
                        json.dumps(decision, ensure_ascii=False),
                    )

                await conn.execute(
                    """
                    UPDATE voice_integration.smart_call_clients
                    SET status = $2,
                        last_outcome = $3,
                        next_callback_at = $4,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    client_id,
                    new_status,
                    outcome,
                    next_callback_at,
                )

        log.info("Обработан session=%s client=%d: %s (delay=%s, status=%s)",
                 session_id, client_id, outcome, delay_minutes, new_status)
        processed += 1

    return processed


async def main() -> None:
    try:
        pool = await get_pool()
    except Exception as e:
        log.exception("Не удалось подключиться к PostgreSQL: %s", e)
        sys.exit(1)

    try:
        processed = await process_completed_calls(pool)
        if processed:
            log.info("Обработано: %d", processed)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
