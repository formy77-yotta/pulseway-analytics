"""
vd_attivita.py — Dashboard Efficienza Tecnici.
Analisi ore eseguite, fatturate, efficienza per operatore e mese.
"""

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from config import DATABASE_URL

st.title("⏱️ Attività Tecnici")

# ------------------------------------------------------------------
# Caricamento dati
# ------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = create_engine(DATABASE_URL)

    attivita = pd.read_sql(
        """
        SELECT
            a.id,
            a.riga,
            a.operatore_id,
            a.cliente_id,
            a.data_attivita,
            a.ore_eseguite,
            a.quantita_fatt,
            a.importo_fatt,
            a.status_fatt,
            a.status_fatt_descr,
            a.tipo_riga,
            a.tipo_addebito
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

    operatori = pd.read_sql("SELECT id, nome FROM dim_operatori", engine)

    return attivita, operatori


try:
    df_att, df_op = load_data()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

if df_att.empty:
    st.info("Nessun dato disponibile in fact_attivita.")
    st.stop()

# ------------------------------------------------------------------
# SIDEBAR — Filtri
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

data_min = df_att["data_attivita"].min().date()
data_max = df_att["data_attivita"].max().date()
default_da = max(data_min, date.today().replace(month=1, day=1))

sel_da = st.sidebar.date_input("Da", value=default_da, min_value=data_min, max_value=data_max)
sel_a  = st.sidebar.date_input("A",  value=data_max,   min_value=data_min, max_value=data_max)

op_map   = df_op.set_index("id")["nome"].to_dict()
op_lista = sorted(op_map.values())
sel_op   = st.sidebar.multiselect("Tecnico", op_lista, default=[])

status_lista = ["Fatturata", "Non fatturabile", "Da fatturare", "Inclusa in contratto"]
sel_status   = st.sidebar.multiselect("Status fatturazione", status_lista, default=[])

target_ore = st.sidebar.number_input(
    "Target ore mensili per tecnico", min_value=1, max_value=500, value=160, step=8
)

# ------------------------------------------------------------------
# Filtro dati
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
if sel_status:
    f = f[f["status_fatt_descr"].isin(sel_status)]

if f.empty:
    st.warning("Nessun dato per i filtri selezionati.")
    st.stop()

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
k2.metric("Ore fatturate", f"{ore_fatt:,.1f} h")
k3.metric("% Efficienza", f"{efficienza:.1f}%")
k4.metric("Importo fatturato", f"€ {importo_fatt:,.0f}".replace(",", "."))
k5.metric("Ore da fatturare", f"{ore_da_fatt:,.1f} h")

st.divider()

# ------------------------------------------------------------------
# Stacked bar: ore per tecnico per mese
# ------------------------------------------------------------------
st.subheader("📅 Ore per Tecnico per Mese")

f["mese"] = f["data_attivita"].dt.to_period("M").astype(str)

STATUS_COLOR = {
    "Fatturata":            "#2ecc71",
    "Non fatturabile":      "#e74c3c",
    "Da fatturare":         "#f39c12",
    "Inclusa in contratto": "#3498db",
    "Non classificato":     "#95a5a6",
}

bar_df = (
    f.groupby(["mese", "operatore_nome", "status_fatt_descr"])["ore_eseguite"]
    .sum()
    .reset_index()
    .sort_values("mese")
)
bar_df["status_fatt_descr"] = bar_df["status_fatt_descr"].fillna("Non classificato")

fig_bar = px.bar(
    bar_df,
    x="mese",
    y="ore_eseguite",
    color="status_fatt_descr",
    facet_col="operatore_nome",
    facet_col_wrap=3,
    color_discrete_map=STATUS_COLOR,
    labels={
        "ore_eseguite":      "Ore",
        "mese":              "Mese",
        "status_fatt_descr": "Status",
        "operatore_nome":    "Tecnico",
    },
    title="Ore eseguite per tecnico e mese (colore = status fatturazione)",
)
fig_bar.update_layout(height=420, margin=dict(t=60, b=10))
fig_bar.update_xaxes(tickangle=30)
st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Tabella efficienza per tecnico
# ------------------------------------------------------------------
st.subheader("👥 Efficienza per Tecnico")

def _sum_by_status(grp: pd.DataFrame, sf: str) -> float:
    return grp.loc[grp["status_fatt"] == sf, "ore_eseguite"].sum()

eff_rows = []
for op, grp in f.groupby("operatore_nome"):
    eff_rows.append({
        "Tecnico":          op,
        "Ore tot.":         grp["ore_eseguite"].sum(),
        "Fatturate":        _sum_by_status(grp, "S"),
        "Non fatt.":        _sum_by_status(grp, "N"),
        "Da fatt.":         _sum_by_status(grp, "A"),
        "Incluse":          _sum_by_status(grp, "I"),
        "Importo fatt. (€)":grp.loc[grp["status_fatt"] == "S", "importo_fatt"].sum(),
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


styled_eff = (
    eff_df.reset_index(drop=True)
    .style.applymap(_eff_color, subset=["Efficienza %"])
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
# Budget vs Consuntivo (ore target per tecnico)
# ------------------------------------------------------------------
st.subheader("🎯 Budget vs Consuntivo Ore")

mesi_nel_periodo = max(
    1,
    (pd.Period(str(sel_a)[:7], "M") - pd.Period(str(sel_da)[:7], "M")).n + 1,
)
budget_totale = float(target_ore * mesi_nel_periodo)

fig_bvc = go.Figure()
for _, row in eff_df.iterrows():
    pct   = (row["Ore tot."] / budget_totale * 100) if budget_totale > 0 else 0.0
    color = "#2ecc71" if pct >= 80 else ("#f39c12" if pct >= 60 else "#e74c3c")
    fig_bvc.add_trace(go.Bar(
        name=row["Tecnico"],
        x=[row["Tecnico"]],
        y=[row["Ore tot."]],
        marker_color=color,
        text=f"{row['Ore tot.']:.0f}h ({pct:.0f}%)",
        textposition="outside",
        showlegend=False,
    ))

fig_bvc.add_hline(
    y=budget_totale,
    line_dash="dash",
    line_color="gray",
    annotation_text=f"Target: {budget_totale:.0f}h ({target_ore}h × {mesi_nel_periodo} mesi)",
    annotation_position="top right",
)
fig_bvc.update_layout(
    height=380,
    yaxis_title="Ore eseguite",
    margin=dict(t=40, b=10),
)
st.plotly_chart(fig_bvc, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Trend efficienza 12 mesi (linea per tecnico)
# ------------------------------------------------------------------
st.subheader("📈 Trend Efficienza Ultimi 12 Mesi")

cutoff_12m = date.today() - timedelta(days=365)
f12 = df_att[df_att["data_attivita"].dt.date >= cutoff_12m].copy()
f12 = f12.merge(
    df_op.rename(columns={"id": "operatore_id", "nome": "operatore_nome"}),
    on="operatore_id",
    how="left",
)
f12["operatore_nome"] = f12["operatore_nome"].fillna("Sconosciuto")
f12["mese"] = f12["data_attivita"].dt.to_period("M").astype(str)

if sel_op:
    f12 = f12[f12["operatore_nome"].isin(sel_op)]

trend_rows = []
for (mese, op), grp in f12.groupby(["mese", "operatore_nome"]):
    ore_t = grp["ore_eseguite"].sum()
    ore_f = grp.loc[grp["status_fatt"] == "S", "ore_eseguite"].sum()
    trend_rows.append({
        "mese":          mese,
        "operatore_nome":op,
        "ore_tot":       ore_t,
        "ore_fatt":      ore_f,
        "efficienza_%":  round(ore_f / ore_t * 100, 1) if ore_t > 0 else float("nan"),
    })

trend_eff = pd.DataFrame(trend_rows).sort_values("mese")

if trend_eff.empty or trend_eff["efficienza_%"].isna().all():
    st.info("Dati insufficienti per il trend efficienza.")
else:
    fig_te = px.line(
        trend_eff,
        x="mese",
        y="efficienza_%",
        color="operatore_nome",
        markers=True,
        labels={
            "efficienza_%":   "Efficienza (%)",
            "mese":           "Mese",
            "operatore_nome": "Tecnico",
        },
    )
    fig_te.add_hline(y=80, line_dash="dot", line_color="#2ecc71",
                     annotation_text="Target 80%", annotation_position="top right")
    fig_te.add_hline(y=60, line_dash="dot", line_color="#e74c3c",
                     annotation_text="Soglia 60%", annotation_position="bottom right")
    fig_te.update_layout(height=360, margin=dict(t=10, b=10), yaxis_range=[0, 110])
    fig_te.update_xaxes(tickangle=30)
    st.plotly_chart(fig_te, use_container_width=True)
