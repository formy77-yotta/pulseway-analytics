import os
from dotenv import load_dotenv

load_dotenv()  # Funziona in locale

def _get(key: str, default: str = "") -> str:
    """Legge da st.secrets (Streamlit Cloud) o os.environ (locale/Railway)."""
    try:
        import streamlit as st
        return st.secrets.get(key, os.environ.get(key, default))
    except Exception:
        return os.environ.get(key, default)

# --- Pulseway PSA ---
SERVER_URL = _get("PULSEWAY_SERVER_URL", "api.psa.pulseway.com")
USERNAME   = _get("PULSEWAY_USERNAME",   "powerbi")
PASSWORD   = _get("PULSEWAY_PASSWORD",   "Yotta2024-")
TENANT     = _get("PULSEWAY_TENANT",     "YottaCore")

# --- PostgreSQL ---
DATABASE_URL = _get("DATABASE_URL")

# --- Paginazione ---
PAGE_SIZE = int(_get("PAGE_SIZE", "100"))

# --- Dashboard ---
DASHBOARD_PASSWORD = _get("DASHBOARD_PASSWORD")
ANTHROPIC_API_KEY  = _get("ANTHROPIC_API_KEY")