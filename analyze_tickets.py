"""
analyze_tickets.py — Analisi AI ticket con Google Gemini
Categorizzazione, rilevamento pattern, qualità assistenza.

Esegui:
    python analyze_tickets.py              # analizza tutti i ticket non ancora analizzati
    python analyze_tickets.py --days 7     # solo ticket degli ultimi 7 giorni
    python analyze_tickets.py --force      # rianalizza tutto
    python analyze_tickets.py --dry-run    # test senza salvare
"""

import re
import json
import time
import argparse
from datetime import datetime, timedelta
from loguru import logger
import psycopg2
import psycopg2.extras
from google import genai
from google.genai import types
from config import DATABASE_URL, GEMINI_API_KEY

# ------------------------------------------------------------------
# CONFIGURAZIONE
# ------------------------------------------------------------------

GEMINI_MODEL  = "gemini-3.1-flash-lite-preview"
BATCH_SIZE    = 15    # ticket per chiamata API
SLEEP_BETWEEN = 2.0   # secondi tra chiamate (rate limiting)

# Categorie standard per i ticket IT
CATEGORIE = [
    "Hardware",
    "Software",
    "Network",
    "Office365",
    "Security",
    "Email",
    "Backup",
    "Server",
    "Postazione",
    "Stampante",
    "Firewall",
    "VPN",
    "Account/Accessi",
    "Monitoraggio",
    "Altro",
]

# ------------------------------------------------------------------
# CONNESSIONE DB
# ------------------------------------------------------------------

def get_conn():
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


# ------------------------------------------------------------------
# SCHEMA TABELLE AI
# ------------------------------------------------------------------

CREATE_AI_TABLES = """
CREATE TABLE IF NOT EXISTS tickets_ai (
    ticket_id               INTEGER PRIMARY KEY,

    -- Categorizzazione AI
    ai_category             TEXT,
    ai_subcategory          TEXT,
    ai_confidence           FLOAT,
    category_match          BOOLEAN,   -- coincide con categoria operatore?
    category_note           TEXT,      -- spiegazione se diversa

    -- Pattern e causa radice
    ai_root_cause           TEXT,
    ai_is_recurring         BOOLEAN,
    ai_pattern_tags         TEXT[],    -- es: ['hardware','ricorrente','stesso_cliente']
    ai_urgency_score        INTEGER,   -- 1-5

    -- Qualità assistenza
    ai_resolution_quality   INTEGER,   -- 1-5 (qualità risoluzione)
    ai_communication_quality INTEGER,  -- 1-5 (qualità comunicazione cliente)
    ai_resolution_clear     BOOLEAN,   -- risoluzione documentata chiaramente?
    ai_quality_notes        TEXT,      -- note sulla qualità

    -- Sommario
    ai_summary              TEXT,      -- sommario breve del ticket
    ai_suggested_action     TEXT,      -- azione suggerita (per ticket aperti)

    -- Metadata
    model_used              TEXT,
    tokens_input            INTEGER,
    tokens_output           INTEGER,
    analyzed_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ticket_clusters (
    id              SERIAL PRIMARY KEY,
    nome            TEXT,
    descrizione     TEXT,
    categoria       TEXT,
    pattern_comune  TEXT,
    n_ticket        INTEGER DEFAULT 0,
    primo_ticket    DATE,
    ultimo_ticket   DATE,
    cliente_id      INTEGER,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ai_analysis_log (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ DEFAULT NOW(),
    tickets_analyzed INTEGER,
    tokens_input    INTEGER,
    tokens_output   INTEGER,
    cost_estimate   FLOAT,
    model_used      TEXT,
    errors          INTEGER
);
"""


def init_ai_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_AI_TABLES)
        conn.commit()
    logger.success("Tabelle AI inizializzate.")


# ------------------------------------------------------------------
# CARICAMENTO DATI
# ------------------------------------------------------------------

def get_tickets_to_analyze(days: int = None, force: bool = False) -> list[dict]:
    """Carica ticket con le loro note per l'analisi."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            date_filter = ""
            params = []

            if days:
                cutoff = datetime.utcnow() - timedelta(days=days)
                date_filter = "AND t.open_date >= %s"
                params.append(cutoff)
            else:
                date_filter = "AND t.open_date >= '2026-01-01'"

            already_filter = "" if force else """
                AND t.id NOT IN (SELECT ticket_id FROM tickets_ai)
            """

            cur.execute(f"""
                SELECT
                    t.id,
                    t.ticket_number,
                    t.title,
                    t.details,
                    t.issue_type_name      AS categoria_operatore,
                    t.sub_issue_type_name  AS subcategoria_operatore,
                    t.status_name,
                    t.priority_name,
                    t.assignee_name,
                    t.account_name,
                    t.open_date,
                    t.completed_date,
                    -- Note aggregate
                    STRING_AGG(
                        CASE WHEN n.note_direction = 'internal'
                             THEN '[TECNICO] ' || n.details_clean
                             ELSE '[CLIENTE] ' || n.details_clean
                        END,
                        ' | '
                        ORDER BY n.created_on
                    ) AS note_testo
                FROM tickets t
                LEFT JOIN ticket_notes n ON n.ticket_id = t.id
                WHERE 1=1
                  {date_filter}
                  {already_filter}
                GROUP BY t.id, t.ticket_number, t.title, t.details,
                         t.issue_type_name, t.sub_issue_type_name,
                         t.status_name, t.priority_name, t.assignee_name,
                         t.account_name, t.open_date, t.completed_date
                ORDER BY t.open_date DESC
            """, params)

            return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------
# PULIZIA TESTO
# ------------------------------------------------------------------

def clean_text(text: str, max_chars: int = 500) -> str:
    """Pulisce e tronca il testo per ridurre i token."""
    if not text:
        return ""
    # Rimuovi HTML residuo
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    # Tronca se troppo lungo
    if len(text) > max_chars:
        text = text[:max_chars] + "..."
    return text


def prepare_ticket_text(ticket: dict) -> dict:
    """Prepara il testo del ticket per l'analisi."""
    return {
        "id":                   ticket["id"],
        "titolo":               clean_text(ticket.get("title", ""), 200),
        "descrizione":          clean_text(ticket.get("details", ""), 300),
        "note":                 clean_text(ticket.get("note_testo", ""), 500),
        "categoria_operatore":  ticket.get("categoria_operatore", ""),
        "stato":                ticket.get("status_name", ""),
        "priorita":             ticket.get("priority_name", ""),
        "cliente":              ticket.get("account_name", ""),
        "chiuso":               ticket.get("completed_date") is not None,
    }


# ------------------------------------------------------------------
# PROMPT GEMINI
# ------------------------------------------------------------------

SYSTEM_PROMPT = f"""
Sei un analista esperto di IT service desk. Analizzi ticket di supporto tecnico
di una società MSP (Managed Service Provider) italiana chiamata Yotta Core.

Categorie disponibili: {', '.join(CATEGORIE)}

Per ogni ticket restituisci SOLO un oggetto JSON valido con questi campi:
- id: (intero, id del ticket)
- ai_category: (stringa, una delle categorie disponibili)
- ai_subcategory: (stringa, sottocategoria specifica es. "RAM guasta", "Password scaduta")
- ai_confidence: (float 0.0-1.0, certezza della categorizzazione)
- category_match: (boolean, true se la categoria AI coincide con quella dell'operatore)
- category_note: (stringa, breve spiegazione se diversa, altrimenti null)
- ai_root_cause: (stringa, causa radice in max 10 parole)
- ai_is_recurring: (boolean, sembra un problema ricorrente?)
- ai_pattern_tags: (array di max 3 stringhe, es: ["hardware","urgente","stessa_sede"])
- ai_urgency_score: (intero 1-5, 5=critico)
- ai_resolution_quality: (intero 1-5, qualità della risoluzione documentata, null se non chiuso)
- ai_communication_quality: (intero 1-5, qualità comunicazione col cliente, null se no note cliente)
- ai_resolution_clear: (boolean, la risoluzione è documentata chiaramente?)
- ai_quality_notes: (stringa, note sulla qualità del servizio, max 20 parole)
- ai_summary: (stringa, sommario del ticket in max 15 parole)
- ai_suggested_action: (stringa, azione suggerita se ticket aperto, null se chiuso)

Rispondi SOLO con un array JSON: [{{...}}, {{...}}]
Nessun testo prima o dopo. JSON valido e completo.
"""


def build_user_prompt(batch: list[dict]) -> str:
    tickets_text = []
    for t in batch:
        ticket_str = (
            f"ID:{t['id']} | "
            f"Titolo: {t['titolo']} | "
            f"Descr: {t['descrizione']} | "
            f"Note: {t['note']} | "
            f"Cat.operatore: {t['categoria_operatore']} | "
            f"Stato: {t['stato']} | "
            f"Chiuso: {t['chiuso']}"
        )
        tickets_text.append(ticket_str)

    return (
        f"Analizza questi {len(batch)} ticket IT:\n\n" +
        "\n---\n".join(tickets_text)
    )


# ------------------------------------------------------------------
# CHIAMATA GEMINI
# ------------------------------------------------------------------

def analyze_batch(client, batch: list[dict], dry_run: bool = False) -> tuple[list[dict], int, int]:
    """
    Invia un batch a Gemini e restituisce (risultati, token_input, token_output).
    """
    prepared = [prepare_ticket_text(t) for t in batch]
    prompt   = build_user_prompt(prepared)

    if dry_run:
        logger.info(f"  [DRY RUN] Batch di {len(batch)} ticket — prompt di {len(prompt)} chars")
        return [], len(prompt) // 4, 0

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                temperature=0.1,
                response_mime_type="application/json",
            ),
        )
        raw_text = response.text.strip()

        # Rimuovi eventuali markdown code blocks
        raw_text = re.sub(r'^```json\s*', '', raw_text)
        raw_text = re.sub(r'^```\s*',     '', raw_text)
        raw_text = re.sub(r'\s*```$',     '', raw_text)
        raw_text = raw_text.strip()

        results = json.loads(raw_text)

        tok_in  = response.usage_metadata.prompt_token_count if response.usage_metadata else 0
        tok_out = response.usage_metadata.candidates_token_count if response.usage_metadata else 0

        return results, tok_in, tok_out

    except json.JSONDecodeError as e:
        logger.error(f"  JSON non valido: {e}\n  Risposta: {raw_text[:300]}")
        return [], 0, 0
    except Exception as e:
        logger.error(f"  Errore Gemini: {e}")
        return [], 0, 0


# ------------------------------------------------------------------
# SALVATAGGIO RISULTATI
# ------------------------------------------------------------------

def save_results(results: list[dict], model_name: str, tok_in: int, tok_out: int):
    if not results:
        return

    sql = """
    INSERT INTO tickets_ai (
        ticket_id, ai_category, ai_subcategory, ai_confidence,
        category_match, category_note,
        ai_root_cause, ai_is_recurring, ai_pattern_tags, ai_urgency_score,
        ai_resolution_quality, ai_communication_quality,
        ai_resolution_clear, ai_quality_notes,
        ai_summary, ai_suggested_action,
        model_used, tokens_input, tokens_output, analyzed_at
    ) VALUES (
        %(id)s, %(ai_category)s, %(ai_subcategory)s, %(ai_confidence)s,
        %(category_match)s, %(category_note)s,
        %(ai_root_cause)s, %(ai_is_recurring)s, %(ai_pattern_tags)s, %(ai_urgency_score)s,
        %(ai_resolution_quality)s, %(ai_communication_quality)s,
        %(ai_resolution_clear)s, %(ai_quality_notes)s,
        %(ai_summary)s, %(ai_suggested_action)s,
        %(model_used)s, %(tokens_input)s, %(tokens_output)s, NOW()
    )
    ON CONFLICT (ticket_id) DO UPDATE SET
        ai_category             = EXCLUDED.ai_category,
        ai_subcategory          = EXCLUDED.ai_subcategory,
        ai_confidence           = EXCLUDED.ai_confidence,
        category_match          = EXCLUDED.category_match,
        category_note           = EXCLUDED.category_note,
        ai_root_cause           = EXCLUDED.ai_root_cause,
        ai_is_recurring         = EXCLUDED.ai_is_recurring,
        ai_pattern_tags         = EXCLUDED.ai_pattern_tags,
        ai_urgency_score        = EXCLUDED.ai_urgency_score,
        ai_resolution_quality   = EXCLUDED.ai_resolution_quality,
        ai_communication_quality = EXCLUDED.ai_communication_quality,
        ai_resolution_clear     = EXCLUDED.ai_resolution_clear,
        ai_quality_notes        = EXCLUDED.ai_quality_notes,
        ai_summary              = EXCLUDED.ai_summary,
        ai_suggested_action     = EXCLUDED.ai_suggested_action,
        model_used              = EXCLUDED.model_used,
        tokens_input            = EXCLUDED.tokens_input,
        tokens_output           = EXCLUDED.tokens_output,
        analyzed_at             = NOW();
    """

    rows = []
    for r in results:
        # Assicura che i campi obbligatori ci siano
        if not r.get("id"):
            continue
        rows.append({
            "id":                       r.get("id"),
            "ai_category":              r.get("ai_category"),
            "ai_subcategory":           r.get("ai_subcategory"),
            "ai_confidence":            r.get("ai_confidence"),
            "category_match":           r.get("category_match"),
            "category_note":            r.get("category_note"),
            "ai_root_cause":            r.get("ai_root_cause"),
            "ai_is_recurring":          r.get("ai_is_recurring"),
            "ai_pattern_tags":          r.get("ai_pattern_tags", []),
            "ai_urgency_score":         r.get("ai_urgency_score"),
            "ai_resolution_quality":    r.get("ai_resolution_quality"),
            "ai_communication_quality": r.get("ai_communication_quality"),
            "ai_resolution_clear":      r.get("ai_resolution_clear"),
            "ai_quality_notes":         r.get("ai_quality_notes"),
            "ai_summary":               r.get("ai_summary"),
            "ai_suggested_action":      r.get("ai_suggested_action"),
            "model_used":               model_name,
            "tokens_input":             tok_in,
            "tokens_output":            tok_out,
        })

    if rows:
        with get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, rows, page_size=50)
            conn.commit()


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

def analyze(days: int = None, force: bool = False, dry_run: bool = False):
    init_ai_tables()

    # Client Gemini (google.genai)
    client = genai.Client(api_key=GEMINI_API_KEY)

    # Carica ticket
    tickets = get_tickets_to_analyze(days=days, force=force)
    logger.info(f"Ticket da analizzare: {len(tickets)}")

    if not tickets:
        logger.info("Nessun ticket da analizzare.")
        return

    # Stima costo
    avg_chars   = 600
    est_tokens  = len(tickets) * avg_chars // 4
    est_cost    = est_tokens / 1_000_000 * 0.25  # $0.25/MTok Gemini 3.1 Flash Lite
    logger.info(f"Stima token: ~{est_tokens:,} | Stima costo: ~${est_cost:.4f}")

    if not dry_run:
        confirm = input(f"\nProcedere con l'analisi di {len(tickets)} ticket? (s/n): ")
        if confirm.lower() != 's':
            logger.info("Analisi annullata.")
            return

    # Suddividi in batch
    batches = [tickets[i:i+BATCH_SIZE] for i in range(0, len(tickets), BATCH_SIZE)]
    logger.info(f"Batch da elaborare: {len(batches)} (da {BATCH_SIZE} ticket ciascuno)")

    total_analyzed  = 0
    total_tok_in    = 0
    total_tok_out   = 0
    total_errors    = 0

    for i, batch in enumerate(batches):
        logger.info(f"Batch {i+1}/{len(batches)} ({len(batch)} ticket)...")

        results, tok_in, tok_out = analyze_batch(client, batch, dry_run=dry_run)

        if results and not dry_run:
            save_results(results, GEMINI_MODEL, tok_in, tok_out)
            total_analyzed += len(results)
        elif dry_run:
            total_analyzed += len(batch)

        total_tok_in  += tok_in
        total_tok_out += tok_out

        if not results and not dry_run:
            total_errors += 1

        # Rate limiting
        if i < len(batches) - 1:
            time.sleep(SLEEP_BETWEEN)

    # Costo reale
    real_cost = (total_tok_in / 1_000_000 * 0.25) + (total_tok_out / 1_000_000 * 1.50)

    logger.success(
        f"\n✅ Analisi completata!\n"
        f"  Ticket analizzati: {total_analyzed}\n"
        f"  Token input:       {total_tok_in:,}\n"
        f"  Token output:      {total_tok_out:,}\n"
        f"  Costo reale:       ${real_cost:.4f}\n"
        f"  Errori batch:      {total_errors}"
    )

    # Salva log
    if not dry_run:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_analysis_log
                        (tickets_analyzed, tokens_input, tokens_output,
                         cost_estimate, model_used, errors)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (total_analyzed, total_tok_in, total_tok_out,
                      real_cost, GEMINI_MODEL, total_errors))
            conn.commit()


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analisi AI ticket con Google Gemini"
    )
    parser.add_argument("--days",    type=int, default=None,
                        help="Analizza solo ticket degli ultimi N giorni")
    parser.add_argument("--force",   action="store_true",
                        help="Rianalizza anche ticket già analizzati")
    parser.add_argument("--dry-run", action="store_true",
                        help="Test senza chiamare Gemini né salvare")
    args = parser.parse_args()

    analyze(days=args.days, force=args.force, dry_run=args.dry_run)
