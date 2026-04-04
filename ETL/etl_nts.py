"""
etl_nts.py — ETL da SQL Server NTS Business Cube → PostgreSQL Railway
Dipendenze: pip install pyodbc psycopg2-binary pandas loguru python-dotenv
Schedula con Task Scheduler Windows ogni notte.
"""

import os
import pyodbc
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------
# CONFIGURAZIONE DA .env
# ------------------------------------------------------------------
SQL_SERVER   = os.environ.get("NTS_SQL_SERVER",   "YCBUS")
SQL_DATABASE = os.environ.get("NTS_SQL_DATABASE",  "YOTTA")
SQL_USER     = os.environ.get("NTS_SQL_USER",      "")
SQL_PASSWORD = os.environ.get("NTS_SQL_PASSWORD",  "")
PG_URL       = os.environ.get("DATABASE_URL",      "")
ANNO_DA      = int(os.environ.get("ANNO_DA",       "2023"))

# ------------------------------------------------------------------
# CONNESSIONI
# ------------------------------------------------------------------

def get_sql_conn():
    """Connessione a SQL Server con utente SQL dedicato."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )
    return pyodbc.connect(conn_str)


def get_pg_conn():
    """Connessione a PostgreSQL Railway."""
    url = PG_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


# ------------------------------------------------------------------
# SCHEMA POSTGRESQL
# ------------------------------------------------------------------

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS dim_clienti (
    nts_id                  INTEGER PRIMARY KEY,
    pulseway_id             INTEGER,
    nome                    TEXT,
    citta                   TEXT,
    provincia               TEXT,
    status                  TEXT,
    tipo                    TEXT,
    cliente_fatturazione    TEXT,
    codice_classificazione1 TEXT,
    codice_classificazione2 TEXT,
    attivo                  BOOLEAN DEFAULT TRUE,
    synced_at               TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dim_clienti_pulseway ON dim_clienti(pulseway_id);
CREATE INDEX IF NOT EXISTS idx_dim_clienti_nome     ON dim_clienti(nome);

CREATE TABLE IF NOT EXISTS dim_operatori (
    id        INTEGER PRIMARY KEY,
    nome      TEXT,
    ruolo     TEXT,
    tipo      TEXT,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_contropartite (
    codice      INTEGER PRIMARY KEY,
    descrizione TEXT,
    tipo        TEXT
);

CREATE TABLE IF NOT EXISTS fact_vendite (
    id            BIGSERIAL PRIMARY KEY,
    anno          INTEGER,
    serie         TEXT,
    numdoc        INTEGER,
    riga          INTEGER,
    cliente_id    INTEGER,
    cliente_fatt_id INTEGER,
    tipo_doc      TEXT,
    data_doc      DATE,
    data_ord      DATE,
    codice_articolo TEXT,
    descrizione   TEXT,
    unita_misura  TEXT,
    quantita      FLOAT,
    prezzo        FLOAT,
    importo       DECIMAL(18,2),
    segno         INTEGER,
    contropartita INTEGER,
    cod_commessa  INTEGER,
    synced_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (anno, serie, numdoc, riga)
);
CREATE INDEX IF NOT EXISTS idx_fact_vendite_cliente  ON fact_vendite(cliente_id);
CREATE INDEX IF NOT EXISTS idx_fact_vendite_data     ON fact_vendite(data_doc);
CREATE INDEX IF NOT EXISTS idx_fact_vendite_anno     ON fact_vendite(anno);
CREATE INDEX IF NOT EXISTS idx_fact_vendite_controp  ON fact_vendite(contropartita);

CREATE TABLE IF NOT EXISTS fact_attivita (
    id              INTEGER,
    riga            INTEGER,
    cliente_id      INTEGER,
    operatore_id    INTEGER,
    data_attivita   DATE,
    oggetto         TEXT,
    note            TEXT,
    luogo           TEXT,
    ore_eseguite    FLOAT,
    quantita_fatt   FLOAT,
    importo_fatt    DECIMAL(18,2),
    status_fatt     TEXT,
    status_fatt_descr TEXT,
    codice_articolo TEXT,
    tipo_riga       TEXT,
    tipo_addebito   TEXT,
    anno_fatt       INTEGER,
    serie_fatt      TEXT,
    numdoc_fatt     INTEGER,
    synced_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, riga)
);
CREATE INDEX IF NOT EXISTS idx_fact_attivita_cliente   ON fact_attivita(cliente_id);
CREATE INDEX IF NOT EXISTS idx_fact_attivita_data      ON fact_attivita(data_attivita);
CREATE INDEX IF NOT EXISTS idx_fact_attivita_operatore ON fact_attivita(operatore_id);
CREATE INDEX IF NOT EXISTS idx_fact_attivita_status    ON fact_attivita(status_fatt);

CREATE TABLE IF NOT EXISTS config_target_ore (
    operatore_id        INTEGER PRIMARY KEY,
    ore_target_mese     FLOAT   NOT NULL DEFAULT 160,
    ore_lavorabili_mese FLOAT   NOT NULL DEFAULT 168,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
"""

STATUS_FATT_MAP = {
    "S": "Fatturata",
    "N": "Non fatturabile",
    "A": "Da fatturare",
    "I": "Inclusa in contratto",
}


def init_db():
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES)
        conn.commit()
    logger.success("Tabelle PostgreSQL inizializzate.")


# ------------------------------------------------------------------
# ETL CLIENTI
# ------------------------------------------------------------------

def etl_clienti():
    logger.info("ETL Clienti...")

    sql = """
    SELECT
        a.an_conto   AS nts_id,
        a.an_descr1  AS nome,
        a.an_tipo    AS tipo,
        a.an_citta   AS citta,
        a.an_prov    AS provincia,
        a.an_status  AS status,
        a.an_codcla1 AS cod_classe1,
        a.an_codcla2 AS cod_classe2,
        CASE
            WHEN a.an_contfatt <> 0 THEN a2.an_descr1
            ELSE a.an_descr1
        END AS cliente_fatturazione
    FROM dbo.anagra a
    LEFT JOIN dbo.anagra a2
        ON a2.an_conto = CASE WHEN a.an_contfatt <> 0 THEN a.an_contfatt ELSE a.an_conto END
    WHERE a.an_tipo IN ('C', 'F')
    """

    with get_sql_conn() as conn:
        df = pd.read_sql(sql, conn)

    logger.info(f"  Letti {len(df)} clienti/fornitori da NTS")

    upsert_sql = """
    INSERT INTO dim_clienti
        (nts_id, nome, tipo, citta, provincia, status,
         cliente_fatturazione, codice_classificazione1, codice_classificazione2, synced_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (nts_id) DO UPDATE SET
        nome                    = EXCLUDED.nome,
        tipo                    = EXCLUDED.tipo,
        citta                   = EXCLUDED.citta,
        provincia               = EXCLUDED.provincia,
        status                  = EXCLUDED.status,
        cliente_fatturazione    = EXCLUDED.cliente_fatturazione,
        codice_classificazione1 = EXCLUDED.codice_classificazione1,
        codice_classificazione2 = EXCLUDED.codice_classificazione2,
        synced_at               = NOW();
    """

    rows = [
        (
            int(r.nts_id),
            str(r.nome).strip()               if r.nome              else None,
            str(r.tipo).strip()               if r.tipo              else None,
            str(r.citta).strip()              if r.citta             else None,
            str(r.provincia).strip()          if r.provincia         else None,
            str(r.status).strip()             if r.status            else None,
            str(r.cliente_fatturazione).strip() if r.cliente_fatturazione else None,
            str(r.cod_classe1).strip()        if r.cod_classe1       else None,
            str(r.cod_classe2).strip()        if r.cod_classe2       else None,
        )
        for r in df.itertuples()
        if r.nts_id is not None
    ]

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
        conn.commit()

    logger.success(f"  Clienti salvati: {len(rows)}")

    # Raccordo automatico con Pulseway per nome simile
    try:
        raccordo_clienti_pulseway()
    except Exception as e:
        logger.warning(f"  Raccordo Pulseway saltato: {e}")


def raccordo_clienti_pulseway():
    """Raccorda dim_clienti con accounts Pulseway per nome simile (pg_trgm)."""
    sql = """
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    UPDATE dim_clienti dc
    SET pulseway_id = sub.account_id
    FROM (
        SELECT dc2.nts_id, t2.account_id,
               similarity(LOWER(dc2.nome), LOWER(t2.account_name)) AS sim
        FROM dim_clienti dc2
        CROSS JOIN (
            SELECT DISTINCT account_id, account_name
            FROM tickets
            WHERE account_name IS NOT NULL
        ) t2
        WHERE similarity(LOWER(dc2.nome), LOWER(t2.account_name)) > 0.5
          AND dc2.pulseway_id IS NULL
    ) sub
    WHERE dc.nts_id = sub.nts_id;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            cur.execute("""
                UPDATE dim_clienti dc
                SET pulseway_id = sub.account_id
                FROM (
                    SELECT dc2.nts_id, t2.account_id,
                           similarity(LOWER(dc2.nome), LOWER(t2.account_name)) AS sim
                    FROM dim_clienti dc2
                    CROSS JOIN (
                        SELECT DISTINCT account_id, account_name
                        FROM tickets
                        WHERE account_name IS NOT NULL
                    ) t2
                    WHERE similarity(LOWER(dc2.nome), LOWER(t2.account_name)) > 0.5
                      AND dc2.pulseway_id IS NULL
                ) sub
                WHERE dc.nts_id = sub.nts_id;
            """)
            updated = cur.rowcount
        conn.commit()
    if updated:
        logger.info(f"  Raccordati {updated} clienti con Pulseway per nome simile")


# ------------------------------------------------------------------
# ETL OPERATORI
# ------------------------------------------------------------------

def etl_operatori():
    logger.info("ETL Operatori...")

    sql = """
    SELECT
        og_progr   AS id,
        og_descont  AS nome,
        og_descont2 AS cognome,
        og_codruaz  AS ruolo,
        og_tipork   AS tipo
    FROM dbo.organig
    WHERE og_codruaz <> 'DDT'
    """

    with get_sql_conn() as conn:
        df = pd.read_sql(sql, conn)

    rows = [
        (
            int(r.id),
            f"{str(r.nome).strip()} {str(r.cognome).strip()}".strip(),
            str(r.ruolo).strip() if r.ruolo else None,
            str(r.tipo).strip()  if r.tipo  else None,
        )
        for r in df.itertuples()
        if r.id is not None
    ]

    upsert_sql = """
    INSERT INTO dim_operatori (id, nome, ruolo, tipo, synced_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (id) DO UPDATE SET
        nome      = EXCLUDED.nome,
        ruolo     = EXCLUDED.ruolo,
        tipo      = EXCLUDED.tipo,
        synced_at = NOW();
    """

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=200)
        conn.commit()

    logger.success(f"  Operatori salvati: {len(rows)}")


# ------------------------------------------------------------------
# ETL CONTROPARTITE
# ------------------------------------------------------------------

def etl_contropartite():
    logger.info("ETL Contropartite da TABCOVE...")

    sql = """
    SELECT
        tb_codcove              AS codice,
        TRIM(tb_descove)        AS descrizione,
        tb_concove              AS conto_contabile
    FROM dbo.TABCOVE
    WHERE tb_codcove IN (
        SELECT DISTINCT mm_controp
        FROM dbo.movmag
        WHERE mm_controp IS NOT NULL AND mm_controp != 0
    )
    """

    CATEGORIA_MAP = {
        5001: ("Servizi spot",     "RICAVO"),
        5002: ("Canoni",           "RICAVO"),
        5003: ("Canoni",           "RICAVO"),
        5004: ("Servizi",          "RICAVO"),
        5005: ("Canoni",           "RICAVO"),
        1007: ("Prodotti",         "RICAVO"),
        4057: ("Ricavi accessori", "RICAVO"),
        4054: ("Omaggi",           "RICAVO"),
        4055: ("Omaggi",           "RICAVO"),
        1006: ("Acquisti",         "COSTO"),
        4060: ("Acquisti",         "COSTO"),
        4062: ("Acquisti",         "COSTO"),
        4063: ("Acquisti",         "COSTO"),
        6002: ("Acquisti canone",  "COSTO"),
        7001: ("Finanziario",      "FINANZIARIO"),
    }

    with get_sql_conn() as conn:
        df = pd.read_sql(sql, conn)

    df = df.drop_duplicates(subset=["codice"], keep="first")

    rows = [
        (
            int(r.codice),
            str(r.descrizione).strip() if r.descrizione else str(r.codice),
            CATEGORIA_MAP.get(int(r.codice), ("Altro", "ALTRO"))[0],
            CATEGORIA_MAP.get(int(r.codice), ("Altro", "ALTRO"))[1],
            str(r.conto_contabile) if r.conto_contabile else None,
        )
        for r in df.itertuples()
        if r.codice is not None
    ]

    upsert_sql = """
    INSERT INTO dim_contropartite (codice, descrizione, categoria, tipo, conto_contabile)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (codice) DO UPDATE SET
        descrizione     = EXCLUDED.descrizione,
        categoria       = EXCLUDED.categoria,
        tipo            = EXCLUDED.tipo,
        conto_contabile = EXCLUDED.conto_contabile;
    """

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE dim_contropartite ADD COLUMN IF NOT EXISTS categoria TEXT")
            cur.execute("ALTER TABLE dim_contropartite ADD COLUMN IF NOT EXISTS conto_contabile TEXT")
            psycopg2.extras.execute_batch(cur, upsert_sql, rows)
        conn.commit()

    logger.success(f"  Contropartite salvate: {len(rows)}")


# ------------------------------------------------------------------
# ETL VENDITE
# ------------------------------------------------------------------

def etl_vendite():
    logger.info(f"ETL Vendite (da anno {ANNO_DA})...")

    sql = f"""
    SELECT
        t.tm_anno                               AS anno,
        TRIM(t.tm_serie)                        AS serie,
        t.tm_numdoc                             AS numdoc,
        m.mm_riga                               AS riga,
        t.tm_conto                              AS cliente_id,
        t.tm_contfatt                           AS cliente_fatt_id,
        t.tm_tipork                             AS tipo_doc,
        CAST(t.tm_datdoc   AS DATE)             AS data_doc,
        CAST(t.tm_datordpa AS DATE)             AS data_ord,
        TRIM(m.mm_codart)                       AS codice_articolo,
        TRIM(m.mm_descr)                        AS descrizione,
        TRIM(m.mm_unmis)                        AS unita_misura,
        m.mm_quant                              AS quantita,
        m.mm_prezzo                             AS prezzo,
        m.mm_controp                            AS contropartita,
        t.tm_codchia                            AS cod_commessa,
        CASE WHEN t.tm_tipork = 'N' THEN -1 ELSE 1 END AS segno,
        ROUND(
            (CASE WHEN t.tm_tipork = 'N' THEN -1 ELSE 1 END
            * m.mm_quant * m.mm_prezzo
            * (100.0 - ISNULL(m.mm_scont1, 0)) / 100
            * (100.0 - ISNULL(m.mm_scont2, 0)) / 100
            * (100.0 - ISNULL(m.mm_scont3, 0)) / 100
            * (100.0 - ISNULL(m.mm_scont4, 0)) / 100
            * (100.0 - ISNULL(m.mm_scont5, 0)) / 100
            * (100.0 - ISNULL(m.mm_scont6, 0)) / 100
            * (100.0 - ISNULL(m.mm_scontp, 0)) / 100
            - ISNULL(m.mm_scontv, 0))
            / NULLIF(m.mm_perqta, 0)
        , 2) AS importo
    FROM dbo.testmag t
    JOIN dbo.movmag m
        ON  m.mm_anno   = t.tm_anno
        AND m.mm_serie  = t.tm_serie
        AND m.mm_numdoc = t.tm_numdoc
        AND m.mm_tipork = t.tm_tipork
    WHERE t.tm_tipork IN ('A', 'N')
      AND NOT (t.tm_tipork = 'B' AND t.tm_tiporkfat = 'A')
      AND t.tm_anno >= {ANNO_DA}
      AND m.mm_codart <> 'D'
      AND m.mm_controp IS NOT NULL
      AND m.mm_controp != 0
    """

    logger.info("  Lettura da SQL Server...")
    with get_sql_conn() as conn:
        df = pd.read_sql(sql, conn)
    logger.info(f"  Lette {len(df)} righe vendite da NTS")

    # Carica clienti validi per evitare FK violation
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nts_id FROM dim_clienti")
            clienti_validi = {row[0] for row in cur.fetchall()}

    upsert_sql = """
    INSERT INTO fact_vendite (
        anno, serie, numdoc, riga,
        cliente_id, cliente_fatt_id,
        tipo_doc, data_doc, data_ord,
        codice_articolo, descrizione, unita_misura,
        quantita, prezzo, importo, segno,
        contropartita, cod_commessa, synced_at
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, NOW()
    )
    ON CONFLICT (anno, serie, numdoc, riga) DO UPDATE SET
        cliente_id    = EXCLUDED.cliente_id,
        importo       = EXCLUDED.importo,
        tipo_doc      = EXCLUDED.tipo_doc,
        contropartita = EXCLUDED.contropartita,
        synced_at     = NOW();
    """

    def _int(v):
        try:
            if pd.isna(v): return None
            return int(v)
        except: return None

    def _float(v):
        try:
            if pd.isna(v): return None
            return float(v)
        except: return None

    def _str(v):
        try:
            if pd.isna(v): return None
            s = str(v).strip()
            return s if s else None
        except: return None

    rows = []
    for r in df.itertuples():
        cid = _int(r.cliente_id)
        # Se cliente non è in dim_clienti, metti None (no FK su fact_vendite)
        rows.append((
            _int(r.anno),
            _str(r.serie) or "",
            _int(r.numdoc),
            _int(r.riga),
            cid if cid in clienti_validi else None,
            _int(r.cliente_fatt_id) if r.cliente_fatt_id and _int(r.cliente_fatt_id) != 0 else None,
            _str(r.tipo_doc),
            r.data_doc if not pd.isna(r.data_doc) else None,
            r.data_ord if not pd.isna(r.data_ord) else None,
            _str(r.codice_articolo),
            _str(r.descrizione),
            _str(r.unita_misura),
            _float(r.quantita),
            _float(r.prezzo),
            _float(r.importo),
            _int(r.segno),
            _int(r.contropartita),
            _int(r.cod_commessa) if r.cod_commessa and _int(r.cod_commessa) != 0 else None,
        ))

    # Filtra righe senza chiave
    rows = [r for r in rows if r[0] and r[2] and r[3]]

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
        conn.commit()

    logger.success(f"  Vendite salvate: {len(rows)}")


# ------------------------------------------------------------------
# ETL ATTIVITA
# ------------------------------------------------------------------

def etl_attivita():
    logger.info(f"ETL Attività (da anno {ANNO_DA})...")

    sql = f"""
    SELECT
        a.ac_codattc                    AS id,
        ISNULL(d.acd_riga, 0)           AS riga,
        a.ac_codlead                    AS cliente_id,
        a.ac_opinc                      AS operatore_id,
        CAST(a.ac_dataesec AS DATE)     AS data_attivita,
        TRIM(a.ac_oggetto)              AS oggetto,
        a.ac_note                       AS note,
        TRIM(a.ac_luogo)                AS luogo,
        d.acd_quantfa                   AS quantita_fatt,
        d.acd_valorefa                  AS importo_fatt,
        a.ac_statusfatt                 AS status_fatt,
        TRIM(d.acd_codart)              AS codice_articolo,
        TRIM(d.acd_tiporiga)            AS tipo_riga,
        TRIM(d.acd_tipoadde)            AS tipo_addebito,
        a.ac_mmanno                     AS anno_fatt,
        TRIM(a.ac_mmserie)              AS serie_fatt,
        a.ac_mmnumdoc                   AS numdoc_fatt
    FROM dbo.attconsc a
    LEFT JOIN dbo.attconscd d ON d.acd_codattc = a.ac_codattc
    WHERE YEAR(a.ac_dataesec) >= {ANNO_DA}
      AND a.ac_dataesec IS NOT NULL
    """

    logger.info("  Lettura da SQL Server...")
    with get_sql_conn() as conn:
        df = pd.read_sql(sql, conn)
    logger.info(f"  Lette {len(df)} righe attività da NTS")

    # Carica operatori validi per evitare dati sporchi
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM dim_operatori")
            operatori_validi = {row[0] for row in cur.fetchall()}

    upsert_sql = """
    INSERT INTO fact_attivita (
        id, riga, cliente_id, operatore_id,
        data_attivita, oggetto, note, luogo,
        ore_eseguite, quantita_fatt, importo_fatt,
        status_fatt, status_fatt_descr,
        codice_articolo, tipo_riga, tipo_addebito,
        anno_fatt, serie_fatt, numdoc_fatt,
        synced_at
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        NOW()
    )
    ON CONFLICT (id, riga) DO UPDATE SET
        ore_eseguite      = EXCLUDED.ore_eseguite,
        status_fatt       = EXCLUDED.status_fatt,
        status_fatt_descr = EXCLUDED.status_fatt_descr,
        importo_fatt      = EXCLUDED.importo_fatt,
        quantita_fatt     = EXCLUDED.quantita_fatt,
        anno_fatt         = EXCLUDED.anno_fatt,
        serie_fatt        = EXCLUDED.serie_fatt,
        numdoc_fatt       = EXCLUDED.numdoc_fatt,
        synced_at         = NOW();
    """

    def _int(v):
        try:
            if pd.isna(v): return None
            return int(v)
        except: return None

    def _float(v):
        try:
            if pd.isna(v): return None
            return float(v)
        except: return None

    def _str(v):
        try:
            if pd.isna(v): return None
            s = str(v).strip()
            return s if s else None
        except: return None

    rows = []
    for r in df.itertuples():
        tid  = _int(r.id)
        riga = _int(r.riga) if _int(r.riga) is not None else 0
        if tid is None:
            continue
        op_id = _int(r.operatore_id)
        # Operatore non valido → None (no FK, solo per sicurezza)
        if op_id and op_id not in operatori_validi:
            op_id = None
        sf = _str(r.status_fatt)
        rows.append((
            tid, riga,
            _int(r.cliente_id) if _int(r.cliente_id) != 0 else None,
            op_id,
            r.data_attivita if not pd.isna(r.data_attivita) else None,
            _str(r.oggetto),
            _str(r.note),
            _str(r.luogo),
            _float(r.quantita_fatt) if _str(r.tipo_riga) == 'O' else 0.0,
            _float(r.quantita_fatt),
            _float(r.importo_fatt),
            sf,
            STATUS_FATT_MAP.get(sf, None) if sf else None,
            _str(r.codice_articolo),
            _str(r.tipo_riga),
            _str(r.tipo_addebito),
            _int(r.anno_fatt) if _int(r.anno_fatt) != 0 else None,
            _str(r.serie_fatt),
            _int(r.numdoc_fatt) if _int(r.numdoc_fatt) != 0 else None,
        ))

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
        conn.commit()

    logger.success(f"  Attività salvate: {len(rows)}")


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

if __name__ == "__main__":
    start = datetime.now()
    logger.info(f"=== ETL NTS → PostgreSQL avviato: {start} ===")

    try:
        init_db()
        etl_clienti()
        etl_operatori()
        etl_contropartite()
        etl_vendite()
        etl_attivita()
        elapsed = (datetime.now() - start).seconds
        logger.success(f"=== ETL completato in {elapsed}s ===")

    except Exception as e:
        logger.error(f"ETL fallito: {e}")
        import traceback
        traceback.print_exc()
        raise