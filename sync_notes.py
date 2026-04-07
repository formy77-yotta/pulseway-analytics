"""
sync_notes.py — Scarica le note dei ticket Pulseway → PostgreSQL
Salva sia note interne (tecniche) che risposte al cliente.
Pulizia automatica del testo automatico di assegnazione.

Esegui:
    python sync_notes.py              # tutti i ticket dal 2026
    python sync_notes.py --days 7     # solo ticket modificati negli ultimi 7 giorni
    python sync_notes.py --force      # riscansiona anche ticket già sincronizzati
"""

import re
import argparse
from datetime import datetime, timedelta
from loguru import logger
import psycopg2
import psycopg2.extras
from api_client import PulsewayClient
from config import DATABASE_URL


# ------------------------------------------------------------------
# CONNESSIONE
# ------------------------------------------------------------------

def get_conn():
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


# ------------------------------------------------------------------
# SCHEMA
# ------------------------------------------------------------------

CREATE_NOTES_TABLE = """
CREATE TABLE IF NOT EXISTS ticket_notes (
    id                  INTEGER PRIMARY KEY,
    ticket_id           INTEGER NOT NULL,
    type_name           TEXT,
    is_internal         BOOLEAN DEFAULT FALSE,
    note_direction      TEXT,        -- 'internal' = nota tecnica, 'customer' = risposta cliente
    details_html        TEXT,        -- testo originale HTML
    details_clean       TEXT,        -- testo pulito senza HTML e testo automatico
    created_by_name     TEXT,
    created_by_email    TEXT,
    created_on          TIMESTAMPTZ,
    modified_on         TIMESTAMPTZ,
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_notes_ticket_id      ON ticket_notes(ticket_id);
CREATE INDEX IF NOT EXISTS idx_notes_direction      ON ticket_notes(note_direction);
CREATE INDEX IF NOT EXISTS idx_notes_created_on     ON ticket_notes(created_on);
CREATE INDEX IF NOT EXISTS idx_notes_is_internal    ON ticket_notes(is_internal);
"""

MIGRATE_SQL = """
ALTER TABLE ticket_notes ADD COLUMN IF NOT EXISTS note_direction TEXT;
ALTER TABLE ticket_notes ADD COLUMN IF NOT EXISTS created_by_email TEXT;
UPDATE ticket_notes 
SET note_direction = CASE WHEN is_internal THEN 'internal' ELSE 'customer' END
WHERE note_direction IS NULL;
"""


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_NOTES_TABLE)
            cur.execute(MIGRATE_SQL)
        conn.commit()
    logger.success("Tabella ticket_notes inizializzata.")


# ------------------------------------------------------------------
# PULIZIA TESTO
# ------------------------------------------------------------------

# Pattern testo automatico / firme / antispam da rimuovere
AUTO_PATTERNS = [
    r'Ticket has been assigned to\s+.*?\s+by\s+[^<\n]+',
    r'Ticket status changed.*?(?=\n|$)',
    r'Ticket priority changed.*?(?=\n|$)',
    r'Ticket queue changed.*?(?=\n|$)',
    # Antispam INKY e simili
    r'Safe\s*Spam\s*Phish\s*More\s*\.{3}\s*FAQ\s*Sicurezza\s*INKY[^\n]*',
    r'INKY\s*sta\s*verificando\s*\.\.\.',
    r'Graymail\s*Spam\s*Phish\s*More\s*\.\.\.',
    r'\[INKY[^\]]*\]',
    # Firme email tipiche
    r'(?:Tel|Mob|Cell|Fax|Phone)[\s.:]*[\+\d\s\-\(\)]{7,20}',
    r'P\.?\s*IVA[\s:]*[\d\s]{11,15}',
    r'C\.?\s*F\.?[\s:]*[A-Za-z0-9]{16}',
    r'Via\s+\w+[\s,]+\d+[\s,]+\d{5}',
    # Separatori di citazione email
    r'Il giorno .{10,50} ha scritto:',
    r'On .{10,50} wrote:',
    # Footer aziendali comuni (tre righe dopo formula di chiusura)
    r'(?:Cordiali saluti|Distinti saluti|Saluti|Regards|Best regards)[^\n]*\n.*?\n.*?\n',
]

# Pattern che possono attraversare più righe (disclaimer, blocchi citazione)
AUTO_PATTERNS_DOTALL = [
    r'(?:Questo messaggio|This email|La presente email).{0,300}(?:riservatezza|confidential|legge)',
    r'(?:Se hai ricevuto|If you received).{0,200}(?:eliminare|delete)',
    r'-{5,}.*?(?:Messaggio originale|Original Message|From:|Da:).*?-{5,}',
]


def clean_html(html: str) -> str:
    """Rimuove HTML, testo automatico, firme, antispam e normalizza."""
    if not html:
        return ""

    text = html

    # Sostituisci <br> e <p> con newline prima di rimuovere i tag
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '', text, flags=re.IGNORECASE)

    # Rimuovi tutti i tag HTML rimanenti
    text = re.sub(r'<[^>]+>', ' ', text)

    # Decodifica entità HTML
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&amp;', '&')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")

    for pattern in AUTO_PATTERNS:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    for pattern in AUTO_PATTERNS_DOTALL:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)

    # Normalizza spazi multipli e newline
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = '\n'.join(line.strip() for line in text.split('\n'))

    # Rimuovi righe troppo corte (probabilmente residui di formattazione)
    lines = text.split('\n')
    lines = [ln for ln in lines if len(ln.split()) >= 3]
    text = '\n'.join(lines)

    # Rimuovi righe duplicate (citazioni ripetute)
    lines = text.split('\n')
    seen: set[str] = set()
    unique_lines: list[str] = []
    for line in lines:
        s = line.strip()
        if s not in seen:
            seen.add(s)
            unique_lines.append(line)
    text = '\n'.join(unique_lines)

    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ------------------------------------------------------------------
# QUERY DB
# ------------------------------------------------------------------

def get_tickets_to_sync(days: int = None) -> list[int]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            if days:
                cutoff = datetime.utcnow() - timedelta(days=days)
                cur.execute("""
                    SELECT id FROM tickets
                    WHERE open_date >= '2026-01-01'
                      AND (last_activity_update >= %s 
                           OR modified_on >= %s
                           OR open_date >= %s)
                    ORDER BY id
                """, (cutoff, cutoff, cutoff))
            else:
                cur.execute("""
                    SELECT id FROM tickets
                    WHERE open_date >= '2026-01-01'
                    ORDER BY id
                """)
            return [row[0] for row in cur.fetchall()]


def get_already_synced() -> set[int]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ticket_id FROM ticket_notes")
            return {row[0] for row in cur.fetchall()}


def upsert_notes(notes: list[dict]):
    if not notes:
        return

    sql = """
    INSERT INTO ticket_notes (
        id, ticket_id, type_name, is_internal, note_direction,
        details_html, details_clean,
        created_by_name, created_by_email,
        created_on, modified_on, synced_at
    ) VALUES (
        %(id)s, %(ticket_id)s, %(type_name)s, %(is_internal)s, %(note_direction)s,
        %(details_html)s, %(details_clean)s,
        %(created_by_name)s, %(created_by_email)s,
        %(created_on)s, %(modified_on)s, NOW()
    )
    ON CONFLICT (id) DO UPDATE SET
        details_html        = EXCLUDED.details_html,
        details_clean       = EXCLUDED.details_clean,
        note_direction      = EXCLUDED.note_direction,
        modified_on         = EXCLUDED.modified_on,
        synced_at           = NOW();
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, notes, page_size=200)
        conn.commit()


# ------------------------------------------------------------------
# MAIN SYNC
# ------------------------------------------------------------------

def sync_notes(days: int = None, force: bool = False):
    init_db()
    client = PulsewayClient()

    ticket_ids = get_tickets_to_sync(days=days)
    logger.info(f"Ticket trovati: {len(ticket_ids)}")

    if not force and not days:
        already = get_already_synced()
        ticket_ids = [t for t in ticket_ids if t not in already]
        logger.info(f"Ticket nuovi da sincronizzare: {len(ticket_ids)}")

    if not ticket_ids:
        logger.info("Nessun ticket da sincronizzare.")
        return

    total_notes  = 0
    total_useful = 0
    errors       = 0

    for i, ticket_id in enumerate(ticket_ids):
        try:
            data = client._get(f"/v2/servicedesk/tickets/{ticket_id}/notes")
            notes_raw = data.get("result", [])

            notes_to_save = []
            for n in notes_raw:
                note_id      = n.get("id")
                details_html = n.get("details", "") or ""
                details_clean = clean_html(details_html)
                is_internal   = n.get("isInternal", True)

                if not note_id:
                    continue

                # Salta note completamente vuote dopo pulizia
                if not details_clean:
                    continue

                notes_to_save.append({
                    "id":               note_id,
                    "ticket_id":        ticket_id,
                    "type_name":        n.get("typeName"),
                    "is_internal":      is_internal,
                    "note_direction":   "internal" if is_internal else "customer",
                    "details_html":     details_html,
                    "details_clean":    details_clean,
                    "created_by_name":  n.get("createdByName"),
                    "created_by_email": n.get("createdByEmail"),
                    "created_on":       n.get("createdOn"),
                    "modified_on":      n.get("modifiedOn"),
                })

            if notes_to_save:
                upsert_notes(notes_to_save)
                total_notes  += len(notes_raw)
                total_useful += len(notes_to_save)

            if (i + 1) % 50 == 0:
                logger.info(
                    f"  Progresso: {i+1}/{len(ticket_ids)} | "
                    f"Note utili: {total_useful}"
                )

        except Exception as e:
            errors += 1
            if "400" not in str(e):  # logga solo errori non-400
                logger.warning(f"  Errore ticket {ticket_id}: {e}")
            continue

    logger.success(
        f"✅ Sync completata! "
        f"Ticket: {len(ticket_ids)} | "
        f"Note totali API: {total_notes} | "
        f"Note utili salvate: {total_useful} | "
        f"Errori: {errors}"
    )

    # Statistiche finali
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ticket_notes")
            tot = cur.fetchone()[0]
            cur.execute(
                "SELECT note_direction, COUNT(*) FROM ticket_notes "
                "GROUP BY note_direction"
            )
            for row in cur.fetchall():
                logger.info(f"  {row[0]}: {row[1]} note")
    logger.info(f"  Totale DB: {tot} note")


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days",  type=int, default=None,
                        help="Solo ticket modificati negli ultimi N giorni")
    parser.add_argument("--force", action="store_true",
                        help="Riscansiona anche ticket già sincronizzati")
    args = parser.parse_args()
    sync_notes(days=args.days, force=args.force)
