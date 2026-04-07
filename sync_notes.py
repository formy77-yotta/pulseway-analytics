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
import html
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

def clean_html(raw: str) -> str:
    """Rimuove HTML, firme, citazioni, INKY, disclaimer, tel/fax/email di rumore."""
    if not raw:
        return ""

    text = raw

    # 1. Decodifica entità HTML
    text = html.unescape(text)

    # 2. Sostituisci tag di blocco con newline
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?(p|div|tr|li)[^>]*>', '\n', text, flags=re.IGNORECASE)

    # 3. Rimuovi tutti i tag HTML rimanenti
    text = re.sub(r'<[^>]+>', ' ', text)

    # 4. Rimuovi testo automatico Pulseway di assegnazione
    text = re.sub(
        r'Ticket has been assigned to\s+.+?\s+by\s+.+?(?=\n|$)',
        '', text, flags=re.IGNORECASE
    )

    # 5. Rimuovi antispam INKY (tutte le varianti)
    text = re.sub(
        r'(?:Safe\s*)?(?:Spam\s*)?(?:Phish\s*)?(?:Graymail\s*)?'
        r'More\s*\.{3}\s*FAQ\s*Sicurezza\s*INKY[^\n]*',
        '', text, flags=re.IGNORECASE
    )
    text = re.sub(r'INKY\s+sta\s+verificando[^\n]*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'(?:Safe|Spam|Phish|Graymail)\s+(?:Safe|Spam|Phish|Graymail)[^\n]*',
                  '', text, flags=re.IGNORECASE)

    # 6. Tronca alla prima citazione/forward trovata
    # Tutto ciò che viene dopo è il messaggio originale citato
    cutoff_patterns = [
        r'\n\s*-{3,}\s*(?:Messaggio originale|Original Message|Forwarded|Inoltrato)',
        r'\nDa:\s+\S+.*\nInviato:',
        r'\nFrom:\s+\S+.*\nSent:',
        r'\nIl giorno .{5,50} ha scritto:',
        r'\nOn .{5,50} wrote:',
        r'\n_{5,}',
        r'\n-{5,}',
    ]
    for pattern in cutoff_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            text = text[:match.start()]

    # 7. Rimuovi firma email (solo negli ultimi 300 caratteri se il testo è lungo)
    firma_patterns = [
        r'\n\s*(?:Cordiali saluti|Distinti saluti|Saluti|Regards|Best regards|Grazie)[^\n]*(?:\n.+){0,5}$',
        r'\n\s*(?:tel|fax|cell|mob)[\s.:]+[\d\s\/\+\-\(\)]{6,}[^\n]*',
    ]
    if len(text) > 300:
        body = text[:-300]
        tail = text[-300:]
        for pattern in firma_patterns:
            tail = re.sub(pattern, '', tail, flags=re.IGNORECASE)
        text = body + tail
    else:
        for pattern in firma_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    # 8. Rimuovi righe che sono SOLO etichetta + numero (tipico di firma)
    text = re.sub(
        r'^\s*(?:tel|fax|cell|mob|telefono|cellulare)[\s.:]+[\+\d\s\-\/\(\)]{6,25}\s*$',
        '', text, flags=re.IGNORECASE | re.MULTILINE
    )

    # 9. Rimuovi indirizzi email nelle firme
    # Mantieni solo email nel corpo del testo (contesto tecnico)
    # Rimuovi righe che contengono SOLO una email
    text = re.sub(r'^\s*[\w\.\-]+@[\w\.\-]+\.\w+\s*$', '', text, flags=re.MULTILINE)

    # 10. Rimuovi "Inviato da Outlook/Mail per iOS/Android"
    text = re.sub(r'Inviato da \w+ per \w+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'Sent from \w+ for \w+', '', text, flags=re.IGNORECASE)

    # 11. Rimuovi disclaimer legali
    disclaimer_starts = [
        'il contenuto di questa',
        'this email is confidential',
        'la presente comunicazione',
        'questo messaggio',
        'if you received this',
        'se hai ricevuto questa',
        'avviso di riservatezza',
        'confidentiality notice',
    ]
    lines = text.split('\n')
    clean_lines = []
    skip = False
    for line in lines:
        line_lower = line.strip().lower()
        if any(line_lower.startswith(d) for d in disclaimer_starts):
            skip = True
        if not skip:
            clean_lines.append(line)
    text = '\n'.join(clean_lines)

    # 12. Pulizia finale
    lines = text.split('\n')
    lines = [l.strip() for l in lines if l.strip()]

    # Rimuovi duplicati consecutivi
    unique_lines = []
    prev = None
    for line in lines:
        if line != prev:
            unique_lines.append(line)
            prev = line

    text = '\n'.join(unique_lines).strip()

    # 13. Normalizza spazi multipli
    text = re.sub(r'[ \t]{2,}', ' ', text)
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
