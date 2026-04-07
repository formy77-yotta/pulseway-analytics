"""
sd_configurazione.py — Configurazione SLA per coda.
Permette di impostare SLA, esclusioni e tipo per ogni coda.
"""

import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
from config import DATABASE_URL

st.title("⚙️ Configurazione SLA per Coda")
st.caption("Imposta SLA target, tipo e parametri di analisi per ogni coda del service desk.")

# ------------------------------------------------------------------
# DB: crea tabella configurazione se non esiste
# ------------------------------------------------------------------

CREATE_CONFIG = """
CREATE TABLE IF NOT EXISTS queue_config (
    queue_name              TEXT PRIMARY KEY,
    sla_prima_risposta_h    FLOAT DEFAULT 4.0,
    sla_risoluzione_h       FLOAT DEFAULT 24.0,
    includi_analisi         BOOLEAN DEFAULT TRUE,
    tipo                    TEXT DEFAULT 'Reattiva',
    note                    TEXT DEFAULT ''
);
"""

# Code predefinite
DEFAULT_QUEUES = {
    "Alerts":                   (1.0,  8.0,  False, "Automatica",  "Ticket generati automaticamente da alert RMM"),
    "Attività Yotta Core":      (4.0,  24.0, True,  "Pianificata", "Attività interne Yotta Core"),
    "FCS - Richieste clienti":  (2.0,  16.0, True,  "Reattiva",    "Richieste dirette dai clienti FCS"),
    "FCS Support":              (2.0,  16.0, True,  "Reattiva",    "Supporto tecnico FCS"),
    "Girardini":                (2.0,  16.0, True,  "Reattiva",    ""),
    "Notifiche - Pennellifaro": (1.0,  8.0,  False, "Automatica",  "Notifiche automatiche"),
    "RMM notifiche":            (1.0,  8.0,  False, "Automatica",  "Notifiche RMM automatiche"),
    "YottaCore Support":        (2.0,  16.0, True,  "Reattiva",    "Supporto principale"),
}


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_config_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_CONFIG)
            # Inserisci code predefinite se non esistono
            for queue, (pr, res, incl, tipo, note) in DEFAULT_QUEUES.items():
                cur.execute("""
                    INSERT INTO queue_config
                        (queue_name, sla_prima_risposta_h, sla_risoluzione_h,
                         includi_analisi, tipo, note)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (queue_name) DO NOTHING
                """, (queue, pr, res, incl, tipo, note))
        conn.commit()


def load_config() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql("SELECT * FROM queue_config ORDER BY queue_name", conn)


def save_config(df: pd.DataFrame):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM queue_config")
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO queue_config
                        (queue_name, sla_prima_risposta_h, sla_risoluzione_h,
                         includi_analisi, tipo, note)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row["queue_name"],
                    float(row["sla_prima_risposta_h"]),
                    float(row["sla_risoluzione_h"]),
                    bool(row["includi_analisi"]),
                    str(row["tipo"]),
                    str(row["note"]) if row["note"] else "",
                ))
        conn.commit()


# ------------------------------------------------------------------
# Init
# ------------------------------------------------------------------
try:
    init_config_table()
    df_config = load_config()
except Exception as e:
    st.error(f"❌ Errore DB: {e}")
    st.stop()

# ------------------------------------------------------------------
# Sidebar — info (nessun filtro sui dati della pagina)
# ------------------------------------------------------------------
st.sidebar.header("ℹ️ Code")
code_escluse = df_config[df_config["includi_analisi"] == False]["queue_name"].astype(str).tolist()  # noqa: E712
if code_escluse:
    st.sidebar.caption(f"⚠️ Code escluse dalle dashboard ticket: {', '.join(code_escluse)}")

# ------------------------------------------------------------------
# Legenda
# ------------------------------------------------------------------
with st.expander("ℹ️ Come funziona", expanded=False):
    st.markdown("""
    | Campo | Descrizione |
    |---|---|
    | **SLA Prima Risposta (h)** | Ore lavorative entro cui il tecnico deve dare prima risposta |
    | **SLA Risoluzione (h)** | Ore lavorative entro cui il ticket deve essere risolto |
    | **Includi Analisi** | Se spuntato, la coda viene inclusa nei grafici e nelle analisi AI |
    | **Tipo** | `Reattiva` = ticket aperti dai clienti, `Pianificata` = attività schedulate, `Automatica` = alert/notifiche |
    | **Note** | Descrizione libera della coda |

    Le code di tipo **Automatica** o con **Includi Analisi = No** vengono escluse dai calcoli
    di tempo medio, SLA e dai report AI.
    """)

# ------------------------------------------------------------------
# Tabella editabile
# ------------------------------------------------------------------
st.subheader("📋 Configurazione Code")

df_edit = df_config.copy()

edited = st.data_editor(
    df_edit,
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    column_config={
        "queue_name": st.column_config.TextColumn(
            "Coda",
            width="medium",
            required=True,
        ),
        "sla_prima_risposta_h": st.column_config.NumberColumn(
            "SLA Prima Risposta (h)",
            min_value=0.5,
            max_value=168.0,
            step=0.5,
            format="%.1f h",
            width="small",
        ),
        "sla_risoluzione_h": st.column_config.NumberColumn(
            "SLA Risoluzione (h)",
            min_value=1.0,
            max_value=720.0,
            step=1.0,
            format="%.0f h",
            width="small",
        ),
        "includi_analisi": st.column_config.CheckboxColumn(
            "Includi Analisi",
            width="small",
        ),
        "tipo": st.column_config.SelectboxColumn(
            "Tipo",
            options=["Reattiva", "Pianificata", "Automatica"],
            width="small",
        ),
        "note": st.column_config.TextColumn(
            "Note",
            width="large",
        ),
    },
)

col_save, col_reset = st.columns([1, 5])

with col_save:
    if st.button("💾 Salva configurazione", type="primary"):
        try:
            save_config(edited)
            st.success("✅ Configurazione salvata!")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"❌ Errore salvataggio: {e}")

with col_reset:
    if st.button("🔄 Ripristina default"):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM queue_config")
                conn.commit()
            init_config_table()
            st.success("✅ Configurazione ripristinata!")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Errore: {e}")

# ------------------------------------------------------------------
# Anteprima SLA
# ------------------------------------------------------------------
st.divider()
st.subheader("📊 Riepilogo SLA Configurati")

col1, col2, col3 = st.columns(3)

incluse = edited[edited["includi_analisi"] == True]
escluse = edited[edited["includi_analisi"] == False]
reattive = edited[edited["tipo"] == "Reattiva"]

col1.metric("Code incluse nell'analisi", len(incluse))
col2.metric("Code escluse", len(escluse))
col3.metric("Code reattive", len(reattive))

if not incluse.empty:
    st.markdown("**Code incluse nell'analisi:**")
    import plotly.express as px
    fig = px.bar(
        incluse.sort_values("sla_risoluzione_h"),
        x="queue_name",
        y=["sla_prima_risposta_h", "sla_risoluzione_h"],
        barmode="group",
        labels={
            "value": "Ore lavorative",
            "queue_name": "Coda",
            "variable": "SLA"
        },
        color_discrete_map={
            "sla_prima_risposta_h": "#00cc96",
            "sla_risoluzione_h": "#636efa"
        },
        title="SLA target per coda (ore lavorative)"
    )
    fig.update_layout(height=350, margin=dict(t=30, b=80))
    fig.update_xaxes(tickangle=30)
    st.plotly_chart(fig, use_container_width=True)
