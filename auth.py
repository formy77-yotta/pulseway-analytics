"""
auth.py — Gestione autenticazione con session_state.
Login una volta sola per tutta la sessione Streamlit.
"""

import streamlit as st
from config import DASHBOARD_PASSWORD


def check_auth():
    """
    Controlla autenticazione. Se la password è già stata inserita
    in questa sessione, non la chiede di nuovo.
    Restituisce True se autenticato, False altrimenti.
    """
    if not DASHBOARD_PASSWORD:
        return True  # Nessuna password configurata

    if st.session_state.get("authenticated"):
        return True

    st.sidebar.markdown("---")
    pwd = st.sidebar.text_input("🔑 Password", type="password", key="pwd_input")

    if pwd:
        if pwd == DASHBOARD_PASSWORD:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.sidebar.error("❌ Password errata")

    if not st.session_state.get("authenticated"):
        st.warning("🔒 Inserisci la password nella sidebar per accedere.")
        st.stop()

    return True
