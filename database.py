"""
database.py — Gestione PostgreSQL per Railway.
Usa psycopg2 e crea le tabelle se non esistono.
"""

import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from loguru import logger
from config import DATABASE_URL


# ------------------------------------------------------------------
# Connessione
# ------------------------------------------------------------------

@contextmanager
def get_conn():
    """Context manager per connessione PostgreSQL."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS tickets (
    id                          INTEGER PRIMARY KEY,
    ticket_number               TEXT,
    title                       TEXT,
    details                     TEXT,

    -- Account / cliente
    account_id                  INTEGER,
    account_name                TEXT,
    account_code                TEXT,
    location_id                 INTEGER,
    location_name               TEXT,

    -- Contatto
    contact_id                  INTEGER,
    contact_name                TEXT,

    -- Assegnatario
    assignee_id                 INTEGER,
    assignee_name               TEXT,

    -- Classificazione
    status_id                   INTEGER,
    status_name                 TEXT,
    priority_id                 INTEGER,
    priority_name               TEXT,
    type_id                     INTEGER,
    type_name                   TEXT,
    issue_type_id               INTEGER,
    issue_type_name             TEXT,
    sub_issue_type_id           INTEGER,
    sub_issue_type_name         TEXT,
    queue_id                    INTEGER,
    queue_name                  TEXT,

    -- Date chiave
    open_date                   TIMESTAMPTZ,
    due_date                    TIMESTAMPTZ,
    completed_date              TIMESTAMPTZ,
    re_opened_date              TIMESTAMPTZ,
    created_on                  TIMESTAMPTZ,
    modified_on                 TIMESTAMPTZ,
    last_activity_update        TIMESTAMPTZ,
    last_status_update          TIMESTAMPTZ,
    last_priority_update        TIMESTAMPTZ,

    -- SLA
    sla_id                      INTEGER,
    sla_name                    TEXT,
    has_met_sla                 INTEGER,
    sla_status_enum             INTEGER,
    is_sla_paused               INTEGER,
    first_response_target_time  TIMESTAMPTZ,
    first_response_actual_time  TIMESTAMPTZ,
    resolution_target_time      TIMESTAMPTZ,
    resolution_actual_time      TIMESTAMPTZ,

    -- Metriche pre-calcolate dall'API (in minuti)
    actual_first_response_min   INTEGER,
    actual_resolution_min       INTEGER,
    actual_pause_min            INTEGER,

    -- Altro
    source_id                   INTEGER,
    contract_id                 INTEGER,
    contract_name               TEXT,
    work_type_id                INTEGER,
    work_type_name              TEXT,
    is_scheduled                INTEGER,
    hardware_asset_id           INTEGER,
    hardware_asset_name         TEXT,
    custom_fields               TEXT,
    custom_contatto_diretto     TEXT,
    custom_fuori_orario         TEXT,

    -- Metadata sync
    synced_at                   TIMESTAMPTZ DEFAULT NOW()
);

-- Indici per performance
CREATE INDEX IF NOT EXISTS idx_tickets_account     ON tickets(account_id);
CREATE INDEX IF NOT EXISTS idx_tickets_assignee    ON tickets(assignee_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status      ON tickets(status_name);
CREATE INDEX IF NOT EXISTS idx_tickets_issue_type  ON tickets(issue_type_name);
CREATE INDEX IF NOT EXISTS idx_tickets_open_date   ON tickets(open_date);
CREATE INDEX IF NOT EXISTS idx_tickets_completed   ON tickets(completed_date);
CREATE INDEX IF NOT EXISTS idx_tickets_priority    ON tickets(priority_name);
"""

CREATE_CATEGORY_MAPPING = """
CREATE TABLE IF NOT EXISTS category_mapping (
    id                  SERIAL PRIMARY KEY,
    pulseway_category   TEXT NOT NULL,
    pulseway_sub        TEXT NOT NULL DEFAULT '',
    ai_category         TEXT,
    ai_subcategory      TEXT,
    is_equivalent       BOOLEAN DEFAULT TRUE,
    note                TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(pulseway_category, pulseway_sub)
);
"""


def init_db():
    """Crea le tabelle se non esistono."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES)
            cur.execute(CREATE_CATEGORY_MAPPING)
    logger.success("Database PostgreSQL inizializzato.")


# ------------------------------------------------------------------
# Upsert tickets
# ------------------------------------------------------------------

UPSERT_TICKET = """
INSERT INTO tickets (
    id, ticket_number, title, details,
    account_id, account_name, account_code, location_id, location_name,
    contact_id, contact_name,
    assignee_id, assignee_name,
    status_id, status_name, priority_id, priority_name,
    type_id, type_name, issue_type_id, issue_type_name,
    sub_issue_type_id, sub_issue_type_name, queue_id, queue_name,
    open_date, due_date, completed_date, re_opened_date,
    created_on, modified_on, last_activity_update,
    last_status_update, last_priority_update,
    sla_id, sla_name, has_met_sla, sla_status_enum, is_sla_paused,
    first_response_target_time, first_response_actual_time,
    resolution_target_time, resolution_actual_time,
    actual_first_response_min, actual_resolution_min, actual_pause_min,
    source_id, contract_id, contract_name,
    work_type_id, work_type_name,
    is_scheduled, hardware_asset_id, hardware_asset_name, custom_fields,
    custom_contatto_diretto, custom_fuori_orario,
    synced_at
) VALUES (
    %(id)s, %(ticket_number)s, %(title)s, %(details)s,
    %(account_id)s, %(account_name)s, %(account_code)s, %(location_id)s, %(location_name)s,
    %(contact_id)s, %(contact_name)s,
    %(assignee_id)s, %(assignee_name)s,
    %(status_id)s, %(status_name)s, %(priority_id)s, %(priority_name)s,
    %(type_id)s, %(type_name)s, %(issue_type_id)s, %(issue_type_name)s,
    %(sub_issue_type_id)s, %(sub_issue_type_name)s, %(queue_id)s, %(queue_name)s,
    %(open_date)s, %(due_date)s, %(completed_date)s, %(re_opened_date)s,
    %(created_on)s, %(modified_on)s, %(last_activity_update)s,
    %(last_status_update)s, %(last_priority_update)s,
    %(sla_id)s, %(sla_name)s, %(has_met_sla)s, %(sla_status_enum)s, %(is_sla_paused)s,
    %(first_response_target_time)s, %(first_response_actual_time)s,
    %(resolution_target_time)s, %(resolution_actual_time)s,
    %(actual_first_response_min)s, %(actual_resolution_min)s, %(actual_pause_min)s,
    %(source_id)s, %(contract_id)s, %(contract_name)s,
    %(work_type_id)s, %(work_type_name)s,
    %(is_scheduled)s, %(hardware_asset_id)s, %(hardware_asset_name)s, %(custom_fields)s,
    %(custom_contatto_diretto)s, %(custom_fuori_orario)s,
    NOW()
)
ON CONFLICT (id) DO UPDATE SET
    status_id                  = EXCLUDED.status_id,
    status_name                = EXCLUDED.status_name,
    assignee_id                = EXCLUDED.assignee_id,
    assignee_name              = EXCLUDED.assignee_name,
    completed_date             = EXCLUDED.completed_date,
    modified_on                = EXCLUDED.modified_on,
    last_activity_update       = EXCLUDED.last_activity_update,
    last_status_update         = EXCLUDED.last_status_update,
    has_met_sla                = EXCLUDED.has_met_sla,
    sla_status_enum            = EXCLUDED.sla_status_enum,
    first_response_actual_time = EXCLUDED.first_response_actual_time,
    resolution_actual_time     = EXCLUDED.resolution_actual_time,
    actual_first_response_min  = EXCLUDED.actual_first_response_min,
    actual_resolution_min      = EXCLUDED.actual_resolution_min,
    actual_pause_min           = EXCLUDED.actual_pause_min,
    biz_minutes_first_response = EXCLUDED.biz_minutes_first_response,
    biz_hours_first_response   = EXCLUDED.biz_hours_first_response,
    biz_minutes_resolution     = EXCLUDED.biz_minutes_resolution,
    biz_hours_resolution       = EXCLUDED.biz_hours_resolution,
    custom_contatto_diretto    = EXCLUDED.custom_contatto_diretto,
    custom_fuori_orario        = EXCLUDED.custom_fuori_orario,
    synced_at                  = NOW();
"""


def _parse_custom(custom_fields_str, key: str):
    """Estrae un valore dal JSON dei custom fields."""
    if not custom_fields_str:
        return None
    try:
        import json
        data = json.loads(custom_fields_str)
        return data.get(key)
    except Exception:
        return None


def _map_ticket(t: dict) -> dict:
    """Mappa campi API (camelCase) → colonne DB (snake_case)."""
    from datetime import datetime
    from business_hours import business_minutes

    def parse_dt(val):
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        try:
            return datetime.fromisoformat(str(val).replace("Z", ""))
        except Exception:
            return None

    open_date  = parse_dt(t.get("openDate"))
    first_resp = parse_dt(t.get("firstResponseActualTime"))
    resolution = parse_dt(t.get("resolutionActualTime"))

    biz_first = business_minutes(open_date, first_resp)
    biz_res   = business_minutes(open_date, resolution)

    return {
        "id":                           t.get("id"),
        "ticket_number":                t.get("ticketNumber"),
        "title":                        t.get("title"),
        "details":                      t.get("details"),
        "account_id":                   t.get("accountId"),
        "account_name":                 t.get("accountName"),
        "account_code":                 t.get("accountCode"),
        "location_id":                  t.get("locationId"),
        "location_name":                t.get("locationName"),
        "contact_id":                   t.get("contactId"),
        "contact_name":                 t.get("contactName"),
        "assignee_id":                  t.get("assigneeId"),
        "assignee_name":                t.get("assigneeName"),
        "status_id":                    t.get("statusId"),
        "status_name":                  t.get("statusName"),
        "priority_id":                  t.get("priorityId"),
        "priority_name":                t.get("priorityName"),
        "type_id":                      t.get("typeId"),
        "type_name":                    t.get("typeName"),
        "issue_type_id":                t.get("issueTypeId"),
        "issue_type_name":              t.get("issueTypeName"),
        "sub_issue_type_id":            t.get("subIssueTypeId"),
        "sub_issue_type_name":          t.get("subIssueTypeName"),
        "queue_id":                     t.get("queueId"),
        "queue_name":                   t.get("queueName"),
        "open_date":                    t.get("openDate"),
        "due_date":                     t.get("dueDate"),
        "completed_date":               t.get("completedDate"),
        "re_opened_date":               t.get("reOpenedDate"),
        "created_on":                   t.get("createdOn"),
        "modified_on":                  t.get("modifiedOn"),
        "last_activity_update":         t.get("lastActivityUpdate"),
        "last_status_update":           t.get("lastStatusUpdate"),
        "last_priority_update":         t.get("lastPriorityUpdate"),
        "sla_id":                       t.get("slaId"),
        "sla_name":                     t.get("slaName"),
        "has_met_sla":                  t.get("hasMetSLA"),
        "sla_status_enum":              t.get("slaStatusEnum"),
        "is_sla_paused":                t.get("isSLAPaused"),
        "first_response_target_time":   t.get("firstResponseTargetTime"),
        "first_response_actual_time":   t.get("firstResponseActualTime"),
        "resolution_target_time":       t.get("resolutionTargetTime"),
        "resolution_actual_time":       t.get("resolutionActualTime"),
        "actual_first_response_min":    t.get("actualFirstResponseMinutes"),
        "actual_resolution_min":        t.get("actualResolutionMinutes"),
        "actual_pause_min":             t.get("actualPauseMinutes"),
        "source_id":                    t.get("sourceId"),
        "contract_id":                  t.get("contractId"),
        "contract_name":                t.get("contractName"),
        "work_type_id":                 t.get("workTypeId"),
        "work_type_name":               t.get("workTypeName"),
        "is_scheduled":                 t.get("isScheduled"),
        "hardware_asset_id":            t.get("hardwareAssetId"),
        "hardware_asset_name":          t.get("hardwareAssetName"),
        "custom_fields":                t.get("customFields"),
        "custom_contatto_diretto":      _parse_custom(t.get("customFields"), "cf_3208"),
        "custom_fuori_orario":          _parse_custom(t.get("customFields"), "cf_3209"),
        "biz_minutes_first_response":   biz_first,
        "biz_hours_first_response":     round(biz_first / 60, 2) if biz_first is not None else None,
        "biz_minutes_resolution":       biz_res,
        "biz_hours_resolution":         round(biz_res / 60, 2) if biz_res is not None else None,
    }


def upsert_tickets(tickets: list[dict]):
    rows = [_map_ticket(t) for t in tickets]
    rows = [r for r in rows if r.get("id") is not None]
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_TICKET, rows, page_size=200)
    logger.info(f"Upsert completato: {len(rows)} ticket salvati.")


def get_ticket_count() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tickets")
            return cur.fetchone()[0]


def delete_removed_tickets(remote_ids: set, from_date: str) -> int:
    """Elimina dal DB i ticket che non esistono più su Pulseway."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM tickets WHERE open_date >= %s::timestamptz",
                (from_date,)
            )
            local_ids = {row[0] for row in cur.fetchall()}
            to_delete = local_ids - remote_ids
            if to_delete:
                cur.execute(
                    "DELETE FROM tickets WHERE id = ANY(%s)",
                    (list(to_delete),)
                )
                return len(to_delete)
    return 0