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


def init_db():
    """Crea le tabelle se non esistono."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES)
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
    synced_at                  = NOW();
"""


def _map_ticket(t: dict) -> dict:
    """Mappa campi API (PascalCase) → colonne DB (snake_case)."""
    return {
        "id":                           t.get("Id"),
        "ticket_number":                t.get("TicketNumber"),
        "title":                        t.get("Title"),
        "details":                      t.get("Details"),
        "account_id":                   t.get("AccountId"),
        "account_name":                 t.get("AccountName"),
        "account_code":                 t.get("AccountCode"),
        "location_id":                  t.get("LocationId"),
        "location_name":                t.get("LocationName"),
        "contact_id":                   t.get("ContactId"),
        "contact_name":                 t.get("ContactName"),
        "assignee_id":                  t.get("AssigneeId"),
        "assignee_name":                t.get("AssigneeName"),
        "status_id":                    t.get("StatusId"),
        "status_name":                  t.get("StatusName"),
        "priority_id":                  t.get("PriorityId"),
        "priority_name":                t.get("PriorityName"),
        "type_id":                      t.get("TypeId"),
        "type_name":                    t.get("TypeName"),
        "issue_type_id":                t.get("IssueTypeId"),
        "issue_type_name":              t.get("IssueTypeName"),
        "sub_issue_type_id":            t.get("SubIssueTypeId"),
        "sub_issue_type_name":          t.get("SubIssueTypeName"),
        "queue_id":                     t.get("QueueId"),
        "queue_name":                   t.get("QueueName"),
        "open_date":                    t.get("OpenDate"),
        "due_date":                     t.get("DueDate"),
        "completed_date":               t.get("CompletedDate"),
        "re_opened_date":               t.get("ReOpenedDate"),
        "created_on":                   t.get("CreatedOn"),
        "modified_on":                  t.get("ModifiedOn"),
        "last_activity_update":         t.get("LastActivityUpdate"),
        "last_status_update":           t.get("LastStatusUpdate"),
        "last_priority_update":         t.get("LastPriorityUpdate"),
        "sla_id":                       t.get("SLAId"),
        "sla_name":                     t.get("SLAName"),
        "has_met_sla":                  t.get("HasMetSLA"),
        "sla_status_enum":              t.get("SLAStatusEnum"),
        "is_sla_paused":                t.get("IsSLAPaused"),
        "first_response_target_time":   t.get("FirstResponseTargetTime"),
        "first_response_actual_time":   t.get("FirstResponseActualTime"),
        "resolution_target_time":       t.get("ResolutionTargetTime"),
        "resolution_actual_time":       t.get("ResolutionActualTime"),
        "actual_first_response_min":    t.get("ActualFirstResponseMinutes"),
        "actual_resolution_min":        t.get("ActualResolutionMinutes"),
        "actual_pause_min":             t.get("ActualPauseMinutes"),
        "source_id":                    t.get("SourceId"),
        "contract_id":                  t.get("ContractId"),
        "contract_name":                t.get("ContractName"),
        "work_type_id":                 t.get("WorkTypeId"),
        "work_type_name":               t.get("WorkTypeName"),
        "is_scheduled":                 t.get("IsScheduled"),
        "hardware_asset_id":            t.get("HardwareAssetId"),
        "hardware_asset_name":          t.get("HardwareAssetName"),
        "custom_fields":                t.get("CustomFields"),
    }


def upsert_tickets(tickets: list[dict]):
    rows = [_map_ticket(t) for t in tickets]
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_TICKET, rows, page_size=200)
    logger.info(f"Upsert completato: {len(rows)} ticket salvati.")


def get_ticket_count() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tickets")
            return cur.fetchone()[0]
