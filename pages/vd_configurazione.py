"""
vd_configurazione.py — Configurazione parametri Vendite.
Target ore fatturabili e ore lavorabili per tecnico.
"""

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from config import DATABASE_URL

st.title("⚙️ Configurazione Vendite")

# ------------------------------------------------------------------
# Caricamento
# ------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_config() -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = create_engine(DATABASE_URL)

    operatori = pd.read_sql(
        "SELECT id, nome, ruolo FROM dim_operatori ORDER BY nome",
        engine,
    )

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS config_target_ore (
                operatore_id        INTEGER PRIMARY KEY,
                ore_target_mese     FLOAT NOT NULL DEFAULT 160,
                ore_lavorabili_mese FLOAT NOT NULL DEFAULT 168,
                updated_at          TIMESTAMPTZ DEFAULT NOW()
            )
        """))

    config = pd.read_sql(
        "SELECT operatore_id, ore_target_mese, ore_lavorabili_mese FROM config_target_ore",
        engine,
    )
    return operatori, config


def save_config(df: pd.DataFrame):
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO config_target_ore
                    (operatore_id, ore_target_mese, ore_lavorabili_mese, updated_at)
                VALUES (:oid, :target, :lav, NOW())
                ON CONFLICT (operatore_id) DO UPDATE SET
                    ore_target_mese     = EXCLUDED.ore_target_mese,
                    ore_lavorabili_mese = EXCLUDED.ore_lavorabili_mese,
                    updated_at          = NOW()
            """), {
                "oid":    int(row["operatore_id"]),
                "target": float(row["ore_target_mese"]),
                "lav":    float(row["ore_lavorabili_mese"]),
            })
    st.cache_data.clear()


try:
    df_op, df_cfg = load_config()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

# ------------------------------------------------------------------
# Target ore per tecnico
# ------------------------------------------------------------------
st.subheader("🎯 Target Ore per Tecnico")
st.caption(
    "**Target ore fatt./mese**: ore fatturabili obiettivo mensile — usato per colorare la pivot nella dashboard Attività.  \n"
    "**Ore lavorabili/mese**: capacità lorda mensile (es. 21 giorni × 8h = 168h)."
)

cfg_full = df_op.rename(columns={"id": "operatore_id"}).merge(
    df_cfg, on="operatore_id", how="left"
)
cfg_full["ore_target_mese"]     = cfg_full["ore_target_mese"].fillna(160.0)
cfg_full["ore_lavorabili_mese"] = cfg_full["ore_lavorabili_mese"].fillna(168.0)

edited = st.data_editor(
    cfg_full[["operatore_id", "nome", "ruolo", "ore_target_mese", "ore_lavorabili_mese"]].rename(
        columns={
            "nome":               "Tecnico",
            "ruolo":              "Ruolo",
            "ore_target_mese":    "Target ore fatt./mese",
            "ore_lavorabili_mese":"Ore lavorabili/mese",
        }
    ),
    column_config={
        "operatore_id":          st.column_config.NumberColumn("ID", disabled=True, width="small"),
        "Tecnico":               st.column_config.TextColumn("Tecnico", disabled=True),
        "Ruolo":                 st.column_config.TextColumn("Ruolo", disabled=True),
        "Target ore fatt./mese": st.column_config.NumberColumn(
            "Target ore fatt./mese",
            help="Ore fatturabili obiettivo al mese",
            min_value=0, max_value=500, step=1, format="%.0f h",
        ),
        "Ore lavorabili/mese":   st.column_config.NumberColumn(
            "Ore lavorabili/mese",
            help="Capacità lorda mensile (es. 168h = 21gg × 8h)",
            min_value=0, max_value=500, step=1, format="%.0f h",
        ),
    },
    hide_index=True,
    use_container_width=True,
    key="cfg_editor",
)

if st.button("💾 Salva configurazione", type="primary"):
    try:
        save_df = edited.rename(columns={
            "Target ore fatt./mese":  "ore_target_mese",
            "Ore lavorabili/mese":    "ore_lavorabili_mese",
        })[["operatore_id", "ore_target_mese", "ore_lavorabili_mese"]]
        save_config(save_df)
        st.success("✅ Configurazione salvata.")
    except Exception as e:
        st.error(f"❌ Errore salvataggio: {e}")
