import os
from dotenv import load_dotenv

load_dotenv()

def _get(key: str, default: str = "") -> str:
    try:
        import streamlit as st
        val = st.secrets.get(key, None)
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(key, default)

SERVER_URL         = _get("PULSEWAY_SERVER_URL", "api.psa.pulseway.com")
USERNAME           = _get("PULSEWAY_USERNAME",   "powerbi")
PASSWORD           = _get("PULSEWAY_PASSWORD",   "Yotta2024-")
TENANT             = _get("PULSEWAY_TENANT",     "YottaCore")
DATABASE_URL       = _get("DATABASE_URL")
PAGE_SIZE          = int(_get("PAGE_SIZE", "100"))
DASHBOARD_PASSWORD = _get("DASHBOARD_PASSWORD")
ANTHROPIC_API_KEY  = _get("ANTHROPIC_API_KEY")
GEMINI_API_KEY     = _get("GEMINI_API_KEY")