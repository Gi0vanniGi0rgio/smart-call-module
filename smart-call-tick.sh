#!/bin/bash
# Обёртка для cron. Запускает Python-планировщик smart-call.
set -e
VENV_PYTHON=/home/ubuntu/smart-call-venv/bin/python3
SCRIPT=/home/ubuntu/smart-call-tick.py
exec "$VENV_PYTHON" "$SCRIPT"
