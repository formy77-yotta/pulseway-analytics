# Pulseway PSA Analytics

Dashboard di analisi ticket — Streamlit + PostgreSQL su Railway.

---

## 🚀 Deploy su Railway (prima volta)

### 1. Crea il repository GitHub
```bash
git init
git add .
git commit -m "first commit"
# Crea un repo su github.com e poi:
git remote add origin https://github.com/TUO-USERNAME/pulseway-analytics.git
git push -u origin main
```

### 2. Crea il progetto su Railway
1. Vai su [railway.app](https://railway.app) e fai login
2. **New Project → Deploy from GitHub repo** → seleziona il tuo repo
3. Railway fa il deploy automaticamente ✅

### 3. Aggiungi PostgreSQL
1. Nel progetto Railway → **New** → **Database** → **PostgreSQL**
2. Clicca sul database → tab **Connect** → copia la stringa `DATABASE_URL`

### 4. Imposta le variabili d'ambiente
Nel servizio della dashboard → tab **Variables** → aggiungi:

| Variabile | Valore |
|---|---|
| `PULSEWAY_SERVER_URL` | `api.psa.pulseway.com` |
| `PULSEWAY_USERNAME` | `powerbi` |
| `PULSEWAY_PASSWORD` | `Yotta2024-` |
| `PULSEWAY_TENANT` | `YottaCore` |
| `DATABASE_URL` | *(incolla da PostgreSQL → Connect)* |
| `DASHBOARD_PASSWORD` | *(scegli una password per la tua collega)* |

> Railway inietta `DATABASE_URL` automaticamente se i servizi sono nello stesso progetto,
> ma impostarlo esplicitamente è più sicuro.

### 5. Primo sync (carica tutti i ticket storici)
Nel terminale locale:
```bash
pip install -r requirements.txt
cp .env.example .env   # modifica con la tua DATABASE_URL reale
python sync.py
```

### 6. Configura il cron job per sync automatico
In Railway → **New** → **Cron Job**:
- **Command:** `python sync.py --days 2`
- **Schedule:** `0 3 * * *` (ogni notte alle 3:00)
- Collega le stesse **Variables** del servizio dashboard

---

## 💻 Sviluppo locale con Cursor

```bash
# 1. Crea ambiente virtuale
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux

# 2. Installa dipendenze
pip install -r requirements.txt

# 3. Configura .env
cp .env.example .env
# Modifica .env con la tua DATABASE_URL da Railway

# 4. Primo sync
python sync.py

# 5. Avvia dashboard
streamlit run dashboard.py
# → http://localhost:8501
```

### Aggiornamenti: push → deploy automatico
```bash
git add .
git commit -m "miglioria dashboard"
git push
# Railway fa il redeploy in automatico ✅
```

---

## 📁 Struttura file

| File | Descrizione |
|---|---|
| `config.py` | Legge credenziali da variabili d'ambiente / `.env` |
| `api_client.py` | Client HTTP Pulseway con auth e paginazione |
| `database.py` | Schema PostgreSQL e funzioni upsert |
| `sync.py` | Scarica ticket e li salva nel DB |
| `dashboard.py` | App Streamlit con tutti i grafici |
| `Procfile` | Dice a Railway come avviare la dashboard |
| `railway.json` | Configurazione deploy Railway |
| `.env.example` | Template variabili d'ambiente locali |

---

## 📊 Grafici disponibili

- 📈 Trend mensile aperti/chiusi
- 🔵 Distribuzione per stato
- 🏷️ Ticket per categoria (IssueType)
- ⚡ Per priorità
- 👤 Carico e mediana ore chiusura per tecnico
- 🏢 Top clienti per volume
- 🎯 SLA rispettato vs violato (%)
- ⏱️ Distribuzione tempi chiusura e prima risposta
