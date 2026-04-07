"""
Filtro ticket tramite queue_config.includi_analisi per le dashboard Service Desk.

Se queue_config è vuota, non ha code con includi_analisi = TRUE, o la tabella
manca, le query nelle pagine usano un fallback (tutti i ticket) gestito nel
caricamento con try/except.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def load_queue_config_dataframe(engine: Engine) -> pd.DataFrame:
    """Legge queue_name / includi_analisi. Tabella assente → DataFrame vuoto."""
    try:
        return pd.read_sql(
            "SELECT queue_name, includi_analisi FROM queue_config ORDER BY queue_name",
            engine,
        )
    except Exception:
        return pd.DataFrame(columns=["queue_name", "includi_analisi"])

# Condizione su alias `t` (tabella tickets)
QUEUE_FILTER_WHERE = """(
    (SELECT COUNT(*)::int FROM queue_config) = 0
    OR NOT EXISTS (
        SELECT 1 FROM queue_config WHERE includi_analisi = TRUE
    )
    OR t.queue_name IN (
        SELECT queue_name FROM queue_config
        WHERE includi_analisi = TRUE
    )
)"""


def sql_tickets_filtered() -> str:
    return f"SELECT t.* FROM tickets t WHERE {QUEUE_FILTER_WHERE}"


def sql_ai_categories_join_filtered() -> str:
    return f"""
    SELECT
        t.id,
        t.ticket_number,
        t.title,
        t.account_name,
        t.assignee_name,
        t.issue_type_name,
        t.status_name,
        t.priority_name,
        t.open_date,
        t.completed_date,
        ai.ai_category,
        ai.ai_subcategory,
        ai.ai_confidence,
        ai.category_match,
        ai.category_note,
        ai.ai_root_cause,
        ai.ai_is_recurring,
        ai.ai_pattern_tags,
        ai.ai_urgency_score,
        ai.ai_resolution_quality,
        ai.ai_communication_quality,
        ai.ai_resolution_clear,
        ai.ai_quality_notes,
        ai.ai_summary,
        ai.ai_suggested_action,
        ai.analyzed_at
    FROM tickets_ai ai
    INNER JOIN tickets t ON t.id = ai.ticket_id
    WHERE {QUEUE_FILTER_WHERE}
    """


def sql_ai_categories_join_unfiltered() -> str:
    """Join tickets_ai ↔ tickets senza filtro code (fallback se la query filtrata fallisce)."""
    return """
    SELECT
        t.id,
        t.ticket_number,
        t.title,
        t.account_name,
        t.assignee_name,
        t.issue_type_name,
        t.status_name,
        t.priority_name,
        t.open_date,
        t.completed_date,
        ai.ai_category,
        ai.ai_subcategory,
        ai.ai_confidence,
        ai.category_match,
        ai.category_note,
        ai.ai_root_cause,
        ai.ai_is_recurring,
        ai.ai_pattern_tags,
        ai.ai_urgency_score,
        ai.ai_resolution_quality,
        ai.ai_communication_quality,
        ai.ai_resolution_clear,
        ai.ai_quality_notes,
        ai.ai_summary,
        ai.ai_suggested_action,
        ai.analyzed_at
    FROM tickets_ai ai
    INNER JOIN tickets t ON t.id = ai.ticket_id
    """
