# Yotta Analytics — Knowledge Base

## 1. Panoramica del progetto

**Scopo:** Dashboard di analisi integrata per Yotta Core. Centralizza dati da due sorgenti distinte (Pulseway PSA e SQL Server NTS) in un unico PostgreSQL su Railway, esposto come app Streamlit multi-pagina.

### Aree coperte
| Area | Sorgente dati | Pagine |
|---|---|---|
| Service Desk | Pulseway PSA API | Dashboard, AI Analytics, Configurazione SLA |
| Vendite / Business | SQL Server NTS (Business Cube) | Fatturato, Clienti, Attività |

### Stack tecnologico
| Layer | Tecnologia |
|---|---|
| Frontend | Streamlit ≥ 1.35 (`st.navigation` multi-pagina) |
| Grafici | Plotly Express ≥ 5.18 |
| Database | PostgreSQL su Railway (psycopg2 + SQLAlchemy) |
| ETL Service Desk | Python `sync.py` (locale o Task Scheduler) |
| ETL Business Cube | Python `etl_nts.py` (su macchina SQL Server, Task Scheduler) |
| Voicebot API | FastAPI + ElevenLabs Conversational AI (Render) |
| AI Analytics | Claude API (`claude-sonnet-4-6`, via `requests`) |
| Hosting dashboard | Streamlit Community Cloud |

### URL e accessi
| Risorsa | URL / Host |
|---|---|
| Dashboard | https://yotta-analytics.streamlit.app |
| PostgreSQL Railway | `hopper.proxy.rlwy.net:43881/railway` |
| Pulseway PSA API | `https://api.psa.pulseway.com` |
| SQL Server NTS | `ycbus.yottacore.local\YOTTA` |
| Voicebot API | Render (vedere `voicebot_api.py`) |

---

## 2. Architettura

```
SQL Server NTS  (ycbus.yottacore.local\YOTTA)
    │  Database: Business Cube — tabelle dim/fact
    │
    ▼  C:\ETL\etl_nts.py
    │  Gira sulla macchina SQL Server via Task Scheduler (ogni ora)
    │  Legge con pyodbc, scrive su PostgreSQL con psycopg2
    │
Pulseway PSA API  (api.psa.pulseway.com)
    │  Auth Bearer JWT — tenant YottaCore, user powerbi
    │  Paginazione GET /v2/servicedesk/tickets
    │
    ▼  sync.py
    │  Gira in locale o su Task Scheduler
    │  Calcola business hours (business_hours.py) durante l'upsert
    │
    ▼
PostgreSQL Railway  (hopper.proxy.rlwy.net:43881/railway)
    │  Tabelle: tickets, queue_config, dim_clienti, dim_operatori,
    │           dim_contropartite, fact_vendite, fact_attivita
    │
    ▼
Streamlit Community Cloud
    └── yotta-analytics.streamlit.app
        app.py → st.navigation() → pages/sd_*, pages/vd_*
```

### Voicebot (componente separato)
```
ElevenLabs Conversational AI
    │  Chiama webhook HTTP con X-API-Key
    ▼
voicebot_api.py  (FastAPI su Render)
    │  Espone: /lookup_contact, /open_tickets, /create_ticket, /account_info
    ▼
api_client.py → Pulseway PSA API
```

---

## 3. Struttura file progetto

```
Yotta-Analytics/
├── app.py                     # Entry point: set_page_config, check_auth, st.navigation()
├── auth.py                    # Login con session_state — controlla DASHBOARD_PASSWORD
├── config.py                  # Variabili d'ambiente (st.secrets → os.environ → default)
├── api_client.py              # PulsewayClient: auth Bearer, paginazione, CRUD ticket, lookup contatti
├── database.py                # Schema PostgreSQL ticket + upsert + delete_removed
├── sync.py                    # Sync Pulseway → PostgreSQL (full o --days N)
├── business_hours.py          # Ore lavorative lun-ven 08:30-17:30, festività IT
├── migrate_business_hours.py  # Migrazione one-shot: aggiunge colonne biz_hours al DB
├── voicebot_api.py            # FastAPI per ElevenLabs voicebot (lookup contatti, crea ticket)
├── requirements.txt           # Dipendenze Python
├── Procfile                   # Per Railway (non più usato)
├── railway.json               # Config Railway (non più usato)
├── .env                       # Variabili locali (NON committare)
├── .env.example               # Template variabili
├── .gitignore
├── KNOWLEDGE_BASE.md          # Questo file
└── pages/
    ├── sd_dashboard.py        # Dashboard ticket Service Desk (KPI, trend, SLA, tecnici)
    ├── sd_ai.py               # AI Analytics ticket — anomalie via Claude API
    ├── sd_configurazione.py   # Configurazione SLA per coda (tabella editabile)
    ├── vd_fatturato.py        # Dashboard Fatturato vendite (KPI YoY, trend, top clienti)
    ├── vd_clienti.py          # Dashboard Clienti [placeholder]
    └── vd_attivita.py         # Dashboard Attività/Ore [placeholder]

ETL/  (cartella nel repo, da copiare/eseguire sulla macchina SQL Server)
├── etl_nts.py                 # ETL NTS Business Cube → PostgreSQL
└── .env                       # Credenziali ETL — NON committato (.gitignore)
```

---

## 4. Database PostgreSQL — Tabelle

### Tabelle Service Desk (da Pulseway PSA)

#### `tickets`
```
id                          INTEGER  PRIMARY KEY    -- ID Pulseway
ticket_number               TEXT                   -- Numero leggibile
title                       TEXT
details                     TEXT

-- Cliente
account_id / account_name / account_code
location_id / location_name
contact_id  / contact_name

-- Assegnatario
assignee_id / assignee_name

-- Classificazione
status_id   / status_name
priority_id / priority_name                        -- Low/Medium/High/Critical
type_id     / type_name
issue_type_id / issue_type_name
sub_issue_type_id / sub_issue_type_name
queue_id    / queue_name

-- Date
open_date, due_date, completed_date, re_opened_date
created_on, modified_on, last_activity_update
last_status_update, last_priority_update

-- SLA
sla_id / sla_name
has_met_sla                 INTEGER  (1=rispettato, 0=violato, NULL=n.d.)
sla_status_enum, is_sla_paused
first_response_target_time / first_response_actual_time
resolution_target_time     / resolution_actual_time

-- Metriche in minuti (pre-calcolate dall'API)
actual_first_response_min
actual_resolution_min
actual_pause_min

-- Business hours (calcolate da business_hours.py al momento dell'upsert)
biz_minutes_first_response  FLOAT   -- minuti lavorativi open→prima risposta
biz_hours_first_response    FLOAT   -- ore lavorative
biz_minutes_resolution      FLOAT   -- minuti lavorativi open→risoluzione
biz_hours_resolution        FLOAT   -- ore lavorative

-- Campi custom (estratti da customFields JSON)
custom_contatto_diretto     TEXT    -- cf_3208: Yes/No
custom_fuori_orario         TEXT    -- cf_3209: Yes/No

-- Altro
source_id, contract_id/name, work_type_id/name
is_scheduled, hardware_asset_id/name
custom_fields               TEXT    -- JSON raw dei custom fields

synced_at                   TIMESTAMPTZ DEFAULT NOW()
```

**Indici:** `account_id`, `assignee_id`, `status_name`, `issue_type_name`, `open_date`, `completed_date`, `priority_name`

#### `queue_config`
```
queue_name              TEXT  PRIMARY KEY
sla_prima_risposta_h    FLOAT DEFAULT 4.0    -- ore lavorative
sla_risoluzione_h       FLOAT DEFAULT 24.0
includi_analisi         BOOLEAN DEFAULT TRUE
tipo                    TEXT                 -- Reattiva / Pianificata / Automatica
note                    TEXT
```
Modificabile dalla pagina `sd_configurazione.py`. Code di tipo `Automatica` o con `includi_analisi=false` vengono escluse dalle analisi.

---

### Tabelle NTS Business Cube (da SQL Server via `etl_nts.py`)

#### `dim_clienti`
```
nts_id                  INTEGER  PRIMARY KEY
pulseway_id             INTEGER              -- FK raccordo con tickets.account_id
nome                    TEXT
citta, provincia        TEXT
status                  TEXT
tipo                    TEXT                 -- C=Cliente / F=Fornitore
cliente_fatturazione    INTEGER              -- FK a nts_id del cliente di fatturazione
codice_classificazione1 TEXT
codice_classificazione2 TEXT
```

#### `dim_operatori`
```
id                      INTEGER  PRIMARY KEY
nome                    TEXT
ruolo                   TEXT
tipo                    TEXT
```

#### `dim_contropartite`
```
codice      INTEGER  PRIMARY KEY   -- codice numerico da dbo.tabmast
descrizione TEXT
tipo        TEXT
    -- RICAVO             → codici 5001, 5005   ricavi principali
    -- RICAVO_ACCESSORIO  → codice 5002          ricavi accessori
    -- RETTIFICA_RICAVO   → codice 5003          note credito / rettifiche
    -- AUTOFATTURA        → codice 5004
    -- CLIENTE            → codici 1006, 1007
    -- IMMOBILIZZAZIONE   → codice 1001
    -- RETTIFICA_COSTO    → codice 6002
    -- PROVENTO           → codice 7001
    -- ALTRO              → tutto il resto (escluso dal fatturato)
```

#### `fact_vendite`
```
anno, serie, numdoc, riga   -- PK composita
cliente_id                  INTEGER  → dim_clienti.nts_id
cliente_fatt_id             INTEGER  → dim_clienti.nts_id (cliente di fatturazione)
tipo_doc                    TEXT     -- A=Fattura / N=Nota credito
data_doc                    DATE
codice_articolo             TEXT
descrizione                 TEXT
unita_misura                TEXT
quantita                    NUMERIC
prezzo                      NUMERIC
importo                     NUMERIC  -- già calcolato con sconti e segno
                                     -- negativo per note credito
segno                       INTEGER  -- +1 / -1
contropartita               TEXT     → dim_contropartite.codice
cod_commessa                TEXT
```

**Logica fatturato:**
```sql
-- Fatturato netto (righe di ricavo):
SELECT SUM(importo) FROM fact_vendite v
JOIN dim_contropartite c ON c.codice = v.contropartita
WHERE c.tipo IN ('RICAVO', 'RICAVO_ACCESSORIO')
  AND v.anno = 2025;
```

#### `fact_attivita`
```
id, riga                    -- PK composita
cliente_id                  INTEGER  → dim_clienti.nts_id
operatore_id                INTEGER  → dim_operatori.id
data_attivita               DATE
oggetto, note               TEXT
luogo                       TEXT
ore_eseguite                NUMERIC
quantita_fatt               NUMERIC
importo_fatt                NUMERIC
status_fatt                 TEXT
    -- S = Fatturata
    -- N = Non fatturabile
    -- A = Da fatturare
    -- I = Inclusa in contratto
codice_articolo             TEXT
anno_fatt, serie_fatt, numdoc_fatt   -- FK a fact_vendite per riconciliazione
```

---

## 5. Sorgenti dati

### Pulseway PSA API

- **Base URL:** `https://api.psa.pulseway.com`
- **Auth:** `POST /v2/security/authenticate` (form-urlencoded) → Bearer JWT
  - `grantType=password`, `userName=powerbi`, `password=…`, `tenant=YottaCore`
  - Token estratto da `response["result"]["accessToken"]`
  - Rinnovo automatico su HTTP 401
- **Endpoint ticket:** `GET /v2/servicedesk/tickets`
  - Paginazione: `PageSize` (default 100), `PageNumber`
  - Filtri usati: `Filter.OpenDateFrom`, `Filter.ExcludeCompleted`, `Filter.ContactId`
- **Endpoint contatti:** `GET /v2/crm/contacts/search` — ricerca per email/nome/cognome
- **Endpoint account:** `GET /v2/crm/accounts/{id}/summaryinfo`
- **Creazione ticket:** `POST /v2/servicedesk/tickets`

**ID fissi rilevanti in Pulseway:**
| Oggetto | ID | Valore |
|---|---|---|
| Priority Low | 40482 | Low |
| Priority Medium | 40483 | Medium |
| Priority High | 40481 | High |
| Priority Critical | 40484 | Critical |
| Status default | 49958 | Nuovo |
| Queue default | 38402 | YottaCore Support |
| Type default | 8 | Incident |
| Source voicebot | 6 | Voice Mail |

**Custom fields:**
| Campo DB | Custom field Pulseway | Valori |
|---|---|---|
| `custom_contatto_diretto` | `cf_3208` | `Yes` / `No` |
| `custom_fuori_orario` | `cf_3209` | `Yes` / `No` |

---

### SQL Server NTS

- **Host:** `ycbus.yottacore.local\YOTTA`
- **Database:** Business Cube (schema NTS)
- **Connessione:** `pyodbc` con Windows Auth o SQL Auth
- **Frequenza ETL:** ogni ora via Task Scheduler su `C:\ETL\etl_nts.py`
- **Strategia:** upsert completo su PostgreSQL (le tabelle dim/fact vengono riallineate ad ogni run)

---

## 6. Script operativi

### `sync.py` — Sync Pulseway → PostgreSQL

```bash
# Sync completo (dal 01/01/2025)
python sync.py

# Sync incrementale (ultimi N giorni)
python sync.py --days 7

# Consigliato in produzione: schedulare ogni ora
# python sync.py --days 2
```

Cosa fa:
1. `init_db()` — crea la tabella `tickets` se non esiste
2. Scarica tutti i ticket via `PulsewayClient.get_all_tickets()`
3. Per ogni ticket calcola `biz_hours_first_response` e `biz_hours_resolution` con `business_hours.py`
4. `upsert_tickets()` — INSERT … ON CONFLICT DO UPDATE (aggiorna solo campi mutevoli: status, assignee, completed_date, SLA, business hours, custom fields)
5. In modalità full: `delete_removed_tickets()` — elimina dal DB i ticket spariti da Pulseway

### `migrate_business_hours.py` — Migrazione one-shot

```bash
python migrate_business_hours.py
```

Eseguire **una volta sola** su un DB esistente senza le colonne `biz_*`. Aggiunge le 4 colonne e ricalcola i valori per tutti i ticket già presenti.

### `ETL/etl_nts.py` — ETL NTS (su macchina SQL Server)

```bash
# In C:\ETL\ sulla macchina ycbus.yottacore.local
python etl_nts.py
```

**Sequenza di esecuzione:**
1. `init_db()` — crea le tabelle dim/fact su PostgreSQL se non esistono
2. `etl_clienti()` — legge `dbo.anagra` (tipo C/F), upsert su `dim_clienti`, poi esegue `raccordo_clienti_pulseway()` con `pg_trgm` (similarity > 0.5) per collegare `pulseway_id`
3. `etl_operatori()` — legge `dbo.organig` (esclude ruolo DDT), upsert su `dim_operatori`
4. `etl_contropartite()` — legge `dbo.tabmast` filtrando solo codici usati in `movmag`, mappa il tipo con `CONTROP_TIPO`
5. `etl_vendite()` — legge `dbo.testmag` + `dbo.movmag` (tipo A/N, anno ≥ ANNO_DA), calcola importo con tutti gli sconti (mm_scont1…6 + scontp + scontv), upsert su `fact_vendite`
6. `etl_attivita()` — legge `dbo.attconsc` + `dbo.attconscd` (anno ≥ ANNO_DA), upsert su `fact_attivita`

**Variabili d'ambiente richieste in `C:\ETL\.env`:**
```
NTS_SQL_SERVER=YCBUS
NTS_SQL_DATABASE=YOTTA
NTS_SQL_USER=YAnalytics
NTS_SQL_PASSWORD=…
DATABASE_URL=postgresql://…@hopper.proxy.rlwy.net:43881/railway
ANNO_DA=2023
```

**Driver richiesto:** ODBC Driver 17 for SQL Server (installato sulla macchina YCBUS)

---

## 7. Calcolo Business Hours

**File:** `business_hours.py`

**Orario lavorativo:** Lunedì–Venerdì, 08:30–17:30 (540 minuti/giorno)

**Festività italiane incluse:**
Capodanno, Epifania, Pasqua, Lunedì dell'Angelo, 25 Aprile, Festa del Lavoro, Repubblica, Ferragosto, Ognissanti, Immacolata, Natale, S. Stefano

**Funzioni esposte:**
```python
business_minutes(start: datetime, end: datetime) -> float | None
business_hours(start: datetime, end: datetime)   -> float | None
```

**Dove vengono calcolate:**
- `biz_hours_first_response` = ore lavorative da `open_date` a `first_response_actual_time`
- `biz_hours_resolution`     = ore lavorative da `open_date` a `resolution_actual_time`
- Calcolate in `database._map_ticket()` durante ogni upsert

---

## 8. Autenticazione dashboard

**File:** `auth.py`

- Se `DASHBOARD_PASSWORD` non è configurata → accesso libero
- Password inserita nella sidebar → salvata in `st.session_state["authenticated"]`
- `check_auth()` è chiamata **una sola volta** in `app.py`; le singole pagine NON la ripetono
- In produzione la password è in `st.secrets` su Streamlit Community Cloud

---

## 9. Configurazione variabili d'ambiente

**File:** `config.py` — priorità: `st.secrets` → `os.environ` → default

| Variabile | Descrizione | Dove |
|---|---|---|
| `DATABASE_URL` | PostgreSQL Railway connection string | Railway + st.secrets |
| `DASHBOARD_PASSWORD` | Password dashboard Streamlit | st.secrets |
| `ANTHROPIC_API_KEY` | Chiave Claude API per AI Analytics | st.secrets |
| `PULSEWAY_SERVER_URL` | `api.psa.pulseway.com` | config default |
| `PULSEWAY_USERNAME` | `powerbi` | config default |
| `PULSEWAY_PASSWORD` | Password account Pulseway | .env / st.secrets |
| `PULSEWAY_TENANT` | `YottaCore` | config default |
| `PAGE_SIZE` | Dimensione pagina API (default 100) | config default |
| `VOICEBOT_API_KEY` | Chiave per autenticare il voicebot | Render env |

**`.env` locale (sviluppo):**
```
DATABASE_URL=postgresql://postgres:…@hopper.proxy.rlwy.net:43881/railway
DASHBOARD_PASSWORD=…
ANTHROPIC_API_KEY=sk-ant-…
PULSEWAY_PASSWORD=…
```

---

## 10. Navigazione app (st.navigation)

```
app.py
├── 🎫 Service Desk
│   ├── 📊 Dashboard         → pages/sd_dashboard.py
│   ├── 🤖 AI Analytics      → pages/sd_ai.py
│   └── ⚙️  Configurazione   → pages/sd_configurazione.py
└── 💰 Vendite
    ├── 📈 Fatturato         → pages/vd_fatturato.py
    ├── 🏢 Clienti           → pages/vd_clienti.py   [WIP]
    └── ⏱️  Attività          → pages/vd_attivita.py  [WIP]
```

**Regola:** `st.set_page_config()` e `check_auth()` vanno **solo** in `app.py`. Le pagine in `pages/` non li ripetono.

---

## 11. Voicebot API

**File:** `voicebot_api.py` — FastAPI deployato su Render

**Endpoint:**
| Metodo | Path | Descrizione |
|---|---|---|
| `POST` | `/lookup_contact` | Cerca contatto per nome/email su Pulseway |
| `POST` | `/open_tickets` | Lista ticket aperti del contatto |
| `POST` | `/create_ticket` | Crea nuovo ticket su Pulseway |
| `GET`  | `/account_info/{id}` | Info azienda |

**Auth:** header `X-API-Key: <VOICEBOT_API_KEY>`

**Integrazione:** ElevenLabs Conversational AI chiama questi endpoint come tool calls durante le conversazioni vocali. Il voicebot può cercare il chiamante, leggere i suoi ticket aperti e aprirne di nuovi, tutto in tempo reale.

---

## 12. Dipendenze Python

```
requests>=2.31.0        # HTTP client (API Pulseway, Claude)
loguru>=0.7.0           # Logging strutturato (sync.py, etl, voicebot)
pandas>=2.0.0           # Manipolazione dati nelle dashboard
streamlit>=1.35.0       # UI dashboard (richiede ≥1.35 per st.navigation)
plotly>=5.18.0          # Grafici interattivi
psycopg2-binary>=2.9.9  # Driver PostgreSQL
sqlalchemy>=2.0.0       # ORM/engine per pandas read_sql nelle dashboard
python-dotenv>=1.0.0    # Caricamento .env in locale
```

Non in requirements.txt (installate a parte nei rispettivi ambienti):
- `fastapi` + `uvicorn` — voicebot su Render
- `pyodbc` — ETL NTS sulla macchina SQL Server
