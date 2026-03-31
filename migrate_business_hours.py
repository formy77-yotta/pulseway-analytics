"""
migrate_business_hours.py — Aggiunge colonne tempi lavorativi al DB
e le calcola per tutti i ticket esistenti.

Esegui una volta sola:
    python migrate_business_hours.py

Poi basta: sync.py le calcolerà automaticamente ad ogni sync.
"""

import pandas as pd
from datetime import datetime
from loguru import logger
from database import get_conn
from business_hours import business_minutes

# ------------------------------------------------------------------
# 1. Aggiungi colonne se non esistono
# ------------------------------------------------------------------

ALTER_COLUMNS = [
    "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS biz_minutes_first_response FLOAT",
    "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS biz_minutes_resolution     FLOAT",
    "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS biz_hours_first_response   FLOAT",
    "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS biz_hours_resolution       FLOAT",
    "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS custom_contatto_diretto TEXT",
    "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS custom_fuori_orario      TEXT",
]


def add_columns():
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in ALTER_COLUMNS:
                cur.execute(sql)
    logger.success("Colonne business hours aggiunte (o già esistenti).")


# ------------------------------------------------------------------
# 2. Calcola e aggiorna tutti i ticket
# ------------------------------------------------------------------

def compute_all():
    logger.info("Carico ticket dal DB...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, open_date, first_response_actual_time, resolution_actual_time
                FROM tickets
                WHERE open_date IS NOT NULL
            """)
            rows = cur.fetchall()

    logger.info(f"Calcolo business hours per {len(rows)} ticket...")

    updates = []
    for row in rows:
        ticket_id, open_date, first_resp, resolution = row

        biz_first = business_minutes(open_date, first_resp)
        biz_res   = business_minutes(open_date, resolution)

        updates.append((
            biz_first,
            round(biz_first / 60, 2) if biz_first is not None else None,
            biz_res,
            round(biz_res / 60, 2)   if biz_res   is not None else None,
            ticket_id,
        ))

    logger.info("Salvo nel DB...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                UPDATE tickets SET
                    biz_minutes_first_response = %s,
                    biz_hours_first_response   = %s,
                    biz_minutes_resolution     = %s,
                    biz_hours_resolution       = %s
                WHERE id = %s
            """, updates)

    logger.success(f"✅ Business hours calcolate per {len(updates)} ticket.")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
if __name__ == "__main__":
    add_columns()
    compute_all()
