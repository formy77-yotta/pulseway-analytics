"""
vd_attivita.py — Dashboard Efficienza Tecnici.
Analisi ore eseguite, fatturate, efficienza per operatore e mese.
"""

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

from config import DATABASE_URL

st.title("⏱️ Attività Tecnici")

# ------------------------------------------------------------------
# Caricamento dati
# ------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    engine = create_engine(DATABASE_URL)

    attivita = pd.read_sql(
        """
        SELECT
            a.id, a.riga, a.operatore_id, a.cliente_id,
            a.data_attivita, a.ore_eseguite, a.quantita_fatt,
            a.importo_fatt, a.status_fatt, a.status_fatt_descr,
            a.tipo_riga, a.tipo_addebito
        FROM fact_attivita a
        WHERE a.tipo_riga = 'O'
          AND a.data_attivita IS NOT NULL
        """,
        engine,
    )
    attivita["data_attivita"] = pd.to_datetime(attivita["data_attivita"], errors="coerce")
    attivita["ore_eseguite"]  = pd.to_numeric(attivita["ore_eseguite"],  errors="coerce").fillna(0.0)
    attivita["quantita_fatt"] = pd.to_numeric(attivita["quantita_fatt"], errors="coerce").fillna(0.0)
    attivita["importo_fatt"]  = pd.to_numeric(attivita["importo_fatt"],  errors="coerce").fillna(0.0)

    operatori = pd.read_sql("SELECT id, nome FROM dim_operatori ORDER BY nome", engine)

    # Config target ore — crea la tabella se non esiste ancora
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

    return attivita, operatori, config


def save_config(df_config: pd.DataFrame):
    """Salva la configurazione target ore su PostgreSQL."""
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        for _, row in df_config.iterrows():
            conn.execute(text("""
                INSERT INTO config_target_ore
                    (operatore_id, ore_target_mese, ore_lavorabili_mese, updated_at)
                VALUES
                    (:oid, :target, :lav, NOW())
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
    df_att, df_op, df_cfg = load_data()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

if df_att.empty:
    st.info("Nessun dato disponibile in fact_attivita.")
    st.stop()

# ------------------------------------------------------------------
# Mappa target per operatore (fallback 160h / 168h se non configurato)
# ------------------------------------------------------------------
cfg_map = df_cfg.set_index("operatore_id").to_dict("index")

def get_target(op_id: int) -> float:
    return cfg_map.get(op_id, {}).get("ore_target_mese", 160.0)

def get_lavorabili(op_id: int) -> float:
    return cfg_map.get(op_id, {}).get("ore_lavorabili_mese", 168.0)

# ------------------------------------------------------------------
# SIDEBAR — Filtri
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

data_min = df_att["data_attivita"].min().date()
data_max = df_att["data_attivita"].max().date()
default_da = max(data_min, date.today().replace(month=1, day=1))

sel_da = st.sidebar.date_input("Da", value=default_da, min_value=data_min, max_value=data_max)
sel_a  = st.sidebar.date_input("A",  value=data_max,   min_value=data_min, max_value=data_max)

op_lista = sorted(df_op["nome"].dropna().tolist())
sel_op   = st.sidebar.multiselect("Tecnico", op_lista, default=[])

# ------------------------------------------------------------------
# Filtro dati base (date + tecnico)
# ------------------------------------------------------------------
f = df_att[
    (df_att["data_attivita"].dt.date >= sel_da) &
    (df_att["data_attivita"].dt.date <= sel_a)
].copy()

f = f.merge(
    df_op.rename(columns={"id": "operatore_id", "nome": "operatore_nome"}),
    on="operatore_id",
    how="left",
)
f["operatore_nome"] = f["operatore_nome"].fillna("Sconosciuto")

if sel_op:
    f = f[f["operatore_nome"].isin(sel_op)]

if f.empty:
    st.warning("Nessun dato per i filtri selezionati.")
    st.stop()

f["mese"] = f["data_attivita"].dt.to_period("M").astype(str)

# ------------------------------------------------------------------
# KPI Principali
# ------------------------------------------------------------------
st.subheader("📊 KPI Principali")

ore_tot      = f["ore_eseguite"].sum()
ore_fatt     = f.loc[f["status_fatt"] == "S", "ore_eseguite"].sum()
ore_da_fatt  = f.loc[f["status_fatt"] == "A", "ore_eseguite"].sum()
importo_fatt = f.loc[f["status_fatt"] == "S", "importo_fatt"].sum()
efficienza   = (ore_fatt / ore_tot * 100) if ore_tot > 0 else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Ore totali eseguite", f"{ore_tot:,.1f} h")
k2.metric("Ore fatturate",       f"{ore_fatt:,.1f} h")
k3.metric("% Efficienza",        f"{efficienza:.1f}%")
k4.metric("Importo fatturato",   f"€ {importo_fatt:,.0f}".replace(",", "."))
k5.metric("Ore da fatturare",    f"{ore_da_fatt:,.1f} h")

st.divider()

# ------------------------------------------------------------------
# Tabella pivot: Tecnici × Mesi
# ------------------------------------------------------------------
st.subheader("📅 Ore per Tecnico per Mese")

PIVOT_OPTIONS = {
    "Tutte":                 None,
    "Fatturate (S)":         "S",
    "Non fatturabili (N)":   "N",
    "Da fatturare (A)":      "A",
    "Incluse in contratto (I)": "I",
}
pivot_sel = st.radio(
    "Visualizza ore:",
    list(PIVOT_OPTIONS.keys()),
    horizontal=True,
    index=0,
)
pivot_status = PIVOT_OPTIONS[pivot_sel]

fp = f if pivot_status is None else f[f["status_fatt"] == pivot_status]

pivot = (
    fp.groupby(["operatore_nome", "mese"])["ore_eseguite"]
    .sum()
    .unstack(fill_value=0.0)
    .sort_index()
)
# Ordina colonne cronologicamente
pivot = pivot[sorted(pivot.columns)]

# Aggiungi colonna Totale riga e riga Totale colonna
pivot["Totale"] = pivot.sum(axis=1)
totale_row = pivot.sum(axis=0).rename("— Totale")
pivot = pd.concat([pivot, totale_row.to_frame().T])

# Mappa target per nome tecnico
op_id_map = df_op.set_index("nome")["id"].to_dict()

def _color_pivot_row(row):
    if row.name == "— Totale":
        return ["font-weight: bold"] * len(row)
    op_name = row.name
    op_id   = op_id_map.get(op_name)
    target  = get_target(op_id) if op_id else 160.0
    styles  = []
    for col, val in row.items():
        if col == "Totale" or val == 0:
            styles.append("")
        elif val >= target:
            styles.append("background-color: #d6f5d6; color: #155724")
        else:
            styles.append("background-color: #ffd6d6; color: #721c24")
    return styles

styled_pivot = (
    pivot.style
    .apply(_color_pivot_row, axis=1)
    .format("{:.1f}", na_rep="—")
)

st.dataframe(styled_pivot, use_container_width=True)

leg_p1, leg_p2, _ = st.columns([1, 1, 4])
leg_p1.markdown('<div style="background:#d6f5d6;padding:4px 10px;border-radius:4px;font-size:0.85em">🟢 ≥ target mensile</div>', unsafe_allow_html=True)
leg_p2.markdown('<div style="background:#ffd6d6;padding:4px 10px;border-radius:4px;font-size:0.85em">🔴 &lt; target mensile</div>', unsafe_allow_html=True)

st.divider()

# ------------------------------------------------------------------
# Tabella efficienza per tecnico
# ------------------------------------------------------------------
st.subheader("👥 Efficienza per Tecnico")


def _sum_sf(grp: pd.DataFrame, sf: str) -> float:
    return grp.loc[grp["status_fatt"] == sf, "ore_eseguite"].sum()


eff_rows = []
for op_nome, grp in f.groupby("operatore_nome"):
    op_id  = op_id_map.get(op_nome)
    target = get_target(op_id) if op_id else 160.0
    mesi_p = max(1, (pd.Period(str(sel_a)[:7], "M") - pd.Period(str(sel_da)[:7], "M")).n + 1)
    eff_rows.append({
        "Tecnico":           op_nome,
        "Ore tot.":          grp["ore_eseguite"].sum(),
        "Fatturate":         _sum_sf(grp, "S"),
        "Non fatt.":         _sum_sf(grp, "N"),
        "Da fatt.":          _sum_sf(grp, "A"),
        "Incluse":           _sum_sf(grp, "I"),
        "Importo fatt. (€)": grp.loc[grp["status_fatt"] == "S", "importo_fatt"].sum(),
        "_target_tot":       target * mesi_p,
    })

eff_df = pd.DataFrame(eff_rows).sort_values("Ore tot.", ascending=False)
eff_df["Efficienza %"] = (
    eff_df["Fatturate"] / eff_df["Ore tot."].replace(0, float("nan")) * 100
).round(1)


def _eff_color(val):
    if pd.isna(val):
        return ""
    if val >= 80:
        return "background-color: #d6f5d6"
    if val >= 60:
        return "background-color: #fff3cd"
    return "background-color: #ffd6d6"


display_eff = eff_df.drop(columns=["_target_tot"]).reset_index(drop=True)
styled_eff = (
    display_eff.style
    .map(_eff_color, subset=["Efficienza %"])
    .format({
        "Ore tot.":          "{:.1f}",
        "Fatturate":         "{:.1f}",
        "Non fatt.":         "{:.1f}",
        "Da fatt.":          "{:.1f}",
        "Incluse":           "{:.1f}",
        "Importo fatt. (€)": "€ {:,.0f}",
        "Efficienza %":      "{:.1f}%",
    }, na_rep="—")
)

st.dataframe(styled_eff, use_container_width=True, height=350)

leg1, leg2, leg3, _ = st.columns([1, 1, 1, 3])
leg1.markdown('<div style="background:#d6f5d6;padding:4px 10px;border-radius:4px;font-size:0.85em">🟢 ≥ 80%</div>', unsafe_allow_html=True)
leg2.markdown('<div style="background:#fff3cd;padding:4px 10px;border-radius:4px;font-size:0.85em">🟡 60–80%</div>', unsafe_allow_html=True)
leg3.markdown('<div style="background:#ffd6d6;padding:4px 10px;border-radius:4px;font-size:0.85em">🔴 &lt; 60%</div>', unsafe_allow_html=True)

st.divider()

# ------------------------------------------------------------------
# Budget vs Consuntivo (target personalizzato per tecnico)
# ------------------------------------------------------------------
st.subheader("🎯 Budget vs Consuntivo Ore")

mesi_nel_periodo = max(
    1,
    (pd.Period(str(sel_a)[:7], "M") - pd.Period(str(sel_da)[:7], "M")).n + 1,
)

fig_bvc = go.Figure()
for _, row in eff_df.iterrows():
    budget = row["_target_tot"]
    pct    = (row["Ore tot."] / budget * 100) if budget > 0 else 0.0
    color  = "#2ecc71" if pct >= 100 else ("#f39c12" if pct >= 80 else "#e74c3c")
    op_id  = op_id_map.get(row["Tecnico"])
    target_m = get_target(op_id) if op_id else 160.0
    fig_bvc.add_trace(go.Bar(
        x=[row["Tecnico"]],
        y=[row["Ore tot."]],
        marker_color=color,
        text=f"{row['Ore tot.']:.0f}h ({pct:.0f}%)",
        textposition="outside",
        showlegend=False,
        customdata=[[budget, target_m, mesi_nel_periodo]],
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Ore eseguite: %{y:.1f}h<br>"
            f"Target: {budget:.0f}h ({target_m:.0f}h × {mesi_nel_periodo} mesi)<br>"
            "vs target: %{text}<extra></extra>"
        ),
    ))

# Linea target per tecnico non è uniforme — mostriamo come annotazioni
for _, row in eff_df.iterrows():
    pass  # target differenti per tecnico gestiti via colori barre

fig_bvc.update_layout(
    height=380,
    yaxis_title="Ore eseguite",
    margin=dict(t=40, b=10),
)
st.plotly_chart(fig_bvc, use_container_width=True)
st.caption("🟢 ≥ 100% target  |  🟡 80–100%  |  🔴 < 80% — target configurabile per tecnico nella sezione sottostante")

st.divider()

# ------------------------------------------------------------------
# Trend efficienza 12 mesi
# ------------------------------------------------------------------
st.subheader("📈 Trend Efficienza Ultimi 12 Mesi")

cutoff_12m = date.today() - timedelta(days=365)
f12 = df_att[df_att["data_attivita"].dt.date >= cutoff_12m].copy()
f12 = f12.merge(
    df_op.rename(columns={"id": "operatore_id", "nome": "operatore_nome"}),
    on="operatore_id", how="left",
)
f12["operatore_nome"] = f12["operatore_nome"].fillna("Sconosciuto")
f12["mese"] = f12["data_attivita"].dt.to_period("M").astype(str)

if sel_op:
    f12 = f12[f12["operatore_nome"].isin(sel_op)]

trend_rows = []
for (mese, op), grp in f12.groupby(["mese", "operatore_nome"]):
    ore_t = grp["ore_eseguite"].sum()
    ore_s = grp.loc[grp["status_fatt"] == "S", "ore_eseguite"].sum()
    trend_rows.append({
        "mese":           mese,
        "operatore_nome": op,
        "ore_tot":        ore_t,
        "ore_fatt":       ore_s,
        "efficienza_%":   round(ore_s / ore_t * 100, 1) if ore_t > 0 else float("nan"),
    })

trend_eff = pd.DataFrame(trend_rows).sort_values("mese")

if trend_eff.empty or trend_eff["efficienza_%"].isna().all():
    st.info("Dati insufficienti per il trend efficienza.")
else:
    fig_te = px.line(
        trend_eff, x="mese", y="efficienza_%", color="operatore_nome",
        markers=True,
        labels={"efficienza_%": "Efficienza (%)", "mese": "Mese", "operatore_nome": "Tecnico"},
    )
    fig_te.add_hline(y=80, line_dash="dot", line_color="#2ecc71",
                     annotation_text="Target 80%", annotation_position="top right")
    fig_te.add_hline(y=60, line_dash="dot", line_color="#e74c3c",
                     annotation_text="Soglia 60%", annotation_position="bottom right")
    fig_te.update_layout(height=360, margin=dict(t=10, b=10), yaxis_range=[0, 110])
    fig_te.update_xaxes(tickangle=30)
    st.plotly_chart(fig_te, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Configurazione target ore per tecnico
# ------------------------------------------------------------------
st.subheader("⚙️ Configurazione Target Ore per Tecnico")

# Costruisci tabella con tutti i tecnici + valori attuali dal DB
cfg_full = df_op.rename(columns={"id": "operatore_id"}).copy()
cfg_full = cfg_full.merge(df_cfg, on="operatore_id", how="left")
cfg_full["ore_target_mese"]     = cfg_full["ore_target_mese"].fillna(160.0)
cfg_full["ore_lavorabili_mese"] = cfg_full["ore_lavorabili_mese"].fillna(168.0)
cfg_full = cfg_full[["operatore_id", "nome", "ore_target_mese", "ore_lavorabili_mese"]]

edited = st.data_editor(
    cfg_full.rename(columns={
        "nome":               "Tecnico",
        "ore_target_mese":    "Target ore fatt./mese",
        "ore_lavorabili_mese":"Ore lavorabili/mese",
    }),
    column_config={
        "operatore_id":            st.column_config.NumberColumn("ID", disabled=True, width="small"),
        "Tecnico":                 st.column_config.TextColumn("Tecnico", disabled=True),
        "Target ore fatt./mese":   st.column_config.NumberColumn(min_value=0, max_value=500, step=1, format="%.0f h"),
        "Ore lavorabili/mese":     st.column_config.NumberColumn(min_value=0, max_value=500, step=1, format="%.0f h"),
    },
    hide_index=True,
    use_container_width=True,
    key="cfg_editor",
)

if st.button("💾 Salva configurazione", type="primary"):
    try:
        save_df = edited.rename(columns={
            "Target ore fatt./mese":   "ore_target_mese",
            "Ore lavorabili/mese":     "ore_lavorabili_mese",
        })[["operatore_id", "ore_target_mese", "ore_lavorabili_mese"]]
        save_config(save_df)
        st.success("✅ Configurazione salvata. La dashboard si aggiornerà al prossimo caricamento.")
    except Exception as e:
        st.error(f"❌ Errore salvataggio: {e}")
