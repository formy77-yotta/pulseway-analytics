# ============================================================
# config.py — Configurazione tramite variabili d'ambiente
# Su Railway le variabili si impostano nella dashboard.
# In locale crea un file .env nella stessa cartella.
# ============================================================

import os
from dotenv import load_dotenv

load_dotenv()  # Carica .env in locale (ignorato su Railway)

# --- Pulseway PSA ---
SERVER_URL = os.environ.get("PULSEWAY_SERVER_URL", "api.psa.pulseway.com")
USERNAME   = os.environ.get("PULSEWAY_USERNAME",   "powerbi")
PASSWORD   = os.environ.get("PULSEWAY_PASSWORD",   "Yotta2024-")
TENANT     = os.environ.get("PULSEWAY_TENANT",     "YottaCore")

# --- PostgreSQL (Railway lo inietta automaticamente come DATABASE_URL) ---
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# --- Paginazione ---
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))

# --- Dashboard auth (opzionale) ---
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
