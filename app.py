import streamlit as st
from auth import check_auth

st.set_page_config(
    page_title="Yotta Analytics",
    page_icon="📊",
    layout="wide",
)

check_auth()

pg = st.navigation({
    "🎫 Service Desk": [
        st.Page("pages/sd_dashboard.py",      title="Dashboard",      icon="📊"),
        st.Page("pages/sd_ai.py",             title="AI Analytics",   icon="🤖"),
        st.Page("pages/sd_configurazione.py", title="Configurazione", icon="⚙️"),
    ],
    "💰 Vendite": [
        st.Page("pages/vd_fatturato.py",       title="Fatturato",      icon="📈"),
        st.Page("pages/vd_clienti.py",         title="Clienti",        icon="🏢"),
        st.Page("pages/vd_attivita.py",        title="Attività",       icon="⏱️"),
        st.Page("pages/vd_configurazione.py",  title="Configurazione", icon="⚙️"),
    ],
})

pg.run()
