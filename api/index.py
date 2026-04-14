"""
Vercel Python serverless entry point.
Adds backend/ to sys.path so all existing modules are importable,
then re-exports the FastAPI `app` object for Vercel's ASGI runner.
"""
import sys
import os
from pathlib import Path

# Make backend/ importable (works both on Vercel and locally)
_backend = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(_backend))

# Load .env for local use — no-op on Vercel (env vars come from dashboard)
try:
    from dotenv import load_dotenv
    load_dotenv(_backend / ".env")
except Exception:
    pass

from main import app  # noqa — Vercel ASGI runner looks for `app`
