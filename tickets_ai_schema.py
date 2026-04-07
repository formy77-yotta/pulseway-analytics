"""Migrazioni schema colonne avanzate su tickets_ai (alert, issues)."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


TICKETS_AI_ALTER_EXTRA: list[str] = [
    "ALTER TABLE tickets_ai ADD COLUMN IF NOT EXISTS security_issues TEXT[]",
    "ALTER TABLE tickets_ai ADD COLUMN IF NOT EXISTS quality_issues TEXT[]",
    "ALTER TABLE tickets_ai ADD COLUMN IF NOT EXISTS communication_issues TEXT[]",
    "ALTER TABLE tickets_ai ADD COLUMN IF NOT EXISTS technical_issues TEXT[]",
    "ALTER TABLE tickets_ai ADD COLUMN IF NOT EXISTS process_issues TEXT[]",
    "ALTER TABLE tickets_ai ADD COLUMN IF NOT EXISTS has_sensitive_data BOOLEAN DEFAULT FALSE",
    "ALTER TABLE tickets_ai ADD COLUMN IF NOT EXISTS alert_level TEXT DEFAULT 'ok'",
]


def apply_tickets_ai_extra_migrations(engine: Engine) -> None:
    """Aggiunge colonne avanzate se mancanti (idempotente)."""
    with engine.begin() as conn:
        for stmt in TICKETS_AI_ALTER_EXTRA:
            conn.execute(text(stmt))
