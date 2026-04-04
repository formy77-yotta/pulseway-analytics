"""
vd_fatturato.py — Dashboard Fatturato Vendite.
Analisi fatturato da fact_vendite + dim_clienti + dim_contropartite.
"""

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from config import DATABASE_URL

st.title("📈 Fatturato")

# ------------------------------------------------------------------
# Caricamento dati
# ------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    engine = create_engine(DATABASE_URL)

    vendite = pd.read_sql("""
        SELECT
            v.anno, v.serie, v.numdoc, v.riga, v.cliente_id,
            v.tipo_doc, v.data_doc, v.codice_articolo, v.descrizione,
            v.unita_misura, v.quantita, v.prezzo, v.importo, v.segno,
            v.contropartita, v.cod_commessa,
            c.nome   AS cliente_nome,
            c.citta, c.provincia, c.tipo AS cliente_tipo,
            cp.descrizione AS contropartita_desc,
            cp.tipo        AS contropartita_tipo
        FROM fact_vendite v
        LEFT JOIN dim_clienti      c  ON c.nts_id  = v.cliente_id
        LEFT JOIN dim_contropartite cp ON cp.codice = v.contropartita
    """, engine)

    clienti       = pd.read_sql("SELECT * FROM dim_clienti",       engine)
    contropartite = pd.read_sql("SELECT * FROM dim_contropartite", engine)

    if "data_doc" in vendite.columns:
        vendite["data_doc"] = pd.to_datetime(vendite["data_doc"], errors="coerce")

    return vendite, clienti, contropartite


try:
    df_raw, df_clienti, df_ctrl = load_data()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

if df_raw.empty:
    st.info("Nessun dato disponibile in fact_vendite.")
    st.stop()

# Solo righe di ricavo per il fatturato principale
RICAVO_TIPI = ("RICAVO", "RICAVO_ACCESSORIO")
df_ricavi = df_raw[df_raw["contropartita_tipo"].isin(RICAVO_TIPI)].copy()

anno_corrente   = date.today().year
anno_precedente = anno_corrente - 1

# ------------------------------------------------------------------
# SIDEBAR — Filtri
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

anni_disponibili = sorted(df_ricavi["anno"].dropna().unique().tolist(), reverse=True)
sel_anni = st.sidebar.multiselect(
    "Anno", anni_disponibili,
    default=[anno_corrente] if anno_corrente in anni_disponibili else anni_disponibili[:1],
)

clienti_lista = ["Tutti"] + sorted(df_ricavi["cliente_nome"].dropna().unique().tolist())
sel_cliente = st.sidebar.selectbox("Cliente", clienti_lista)

ctrl_lista = sorted(df_ricavi["contropartita_desc"].dropna().unique().tolist())
sel_ctrl = st.sidebar.multiselect("Contropartita", ctrl_lista, default=[])

tipo_doc_map = {"Tutti": None, "Solo fatture": "A", "Solo note credito": "N"}
sel_tipo_label = st.sidebar.selectbox("Tipo documento", list(tipo_doc_map.keys()))
sel_tipo = tipo_doc_map[sel_tipo_label]

# Applica filtri
f = df_ricavi.copy()
if sel_anni:
    f = f[f["anno"].isin(sel_anni)]
if sel_cliente != "Tutti":
    f = f[f["cliente_nome"] == sel_cliente]
if sel_ctrl:
    f = f[f["contropartita_desc"].isin(sel_ctrl)]
if sel_tipo:
    f = f[f["tipo_doc"] == sel_tipo]

# ------------------------------------------------------------------
# KPI
# ------------------------------------------------------------------
st.subheader("📊 KPI Principali")

ytd   = df_ricavi[df_ricavi["anno"] == anno_corrente]["importo"].sum()
prec  = df_ricavi[df_ricavi["anno"] == anno_precedente]["importo"].sum()
yoy   = round((ytd - prec) / abs(prec) * 100, 1) if prec else None

# Numero fatture distinte (anno corrente)
n_fatture = (
    df_ricavi[df_ricavi["anno"] == anno_corrente]
    .drop_duplicates(subset=["serie", "numdoc"])
    .shape[0]
)
ticket_medio = round(ytd / n_fatture, 2) if n_fatture else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric(
    f"Fatturato {anno_corrente}",
    f"€ {ytd:,.0f}".replace(",", "."),
)
c2.metric(
    f"Fatturato {anno_precedente}",
    f"€ {prec:,.0f}".replace(",", "."),
)
c3.metric(
    "Variazione YoY",
    f"{yoy:+.1f}%" if yoy is not None else "N/D",
    delta=f"{yoy:+.1f}%" if yoy is not None else None,
)
c4.metric("Fatture emesse", f"{n_fatture:,}".replace(",", "."))
c5.metric("Ticket medio", f"€ {ticket_medio:,.2f}".replace(",", "."))

st.divider()

# ------------------------------------------------------------------
# Trend mensile — anno corrente vs anno precedente
# ------------------------------------------------------------------
st.subheader("📅 Trend Mensile")

def monthly_trend(data: pd.DataFrame, anno: int) -> pd.DataFrame:
    sub = data[data["anno"] == anno].copy()
    sub["mese"] = sub["data_doc"].dt.month
    return (
        sub.groupby("mese")["importo"]
        .sum()
        .reset_index()
        .assign(anno=str(anno))
    )

trend_ac  = monthly_trend(df_ricavi, anno_corrente)
trend_prec = monthly_trend(df_ricavi, anno_precedente)
trend = pd.concat([trend_ac, trend_prec], ignore_index=True)

mese_labels = {
    1:"Gen", 2:"Feb", 3:"Mar", 4:"Apr", 5:"Mag", 6:"Giu",
    7:"Lug", 8:"Ago", 9:"Set", 10:"Ott", 11:"Nov", 12:"Dic",
}
trend["mese_label"] = trend["mese"].map(mese_labels)
trend = trend.sort_values("mese")

fig_trend = px.line(
    trend, x="mese_label", y="importo", color="anno",
    markers=True,
    labels={"importo": "Fatturato (€)", "mese_label": "Mese", "anno": "Anno"},
    color_discrete_map={
        str(anno_corrente):   "#636efa",
        str(anno_precedente): "#ef553b",
    },
)
fig_trend.update_layout(height=340, margin=dict(t=10, b=10))
st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Top clienti + Fatturato per contropartita
# ------------------------------------------------------------------
col_a, col_b = st.columns([3, 2])

with col_a:
    st.subheader("🏢 Top 15 Clienti")
    top_cli = (
        f.groupby("cliente_nome")["importo"]
        .sum()
        .reset_index()
        .sort_values("importo", ascending=False)
        .head(15)
    )
    top_cli.columns = ["Cliente", "Fatturato"]
    fig_cli = px.bar(
        top_cli, x="Fatturato", y="Cliente", orientation="h",
        color="Fatturato", color_continuous_scale="Blues",
        labels={"Fatturato": "€"},
    )
    fig_cli.update_layout(height=420, margin=dict(t=10, b=10), yaxis=dict(autorange="reversed"))
    fig_cli.update_traces(texttemplate="€ %{x:,.0f}", textposition="outside")
    st.plotly_chart(fig_cli, use_container_width=True)

with col_b:
    st.subheader("🏷️ Per Contropartita")
    by_ctrl = (
        f.groupby("contropartita_desc")["importo"]
        .sum()
        .reset_index()
        .sort_values("importo", ascending=False)
    )
    by_ctrl.columns = ["Contropartita", "Fatturato"]
    fig_ctrl = px.pie(
        by_ctrl, values="Fatturato", names="Contropartita",
        hole=0.45,
    )
    fig_ctrl.update_layout(height=420, margin=dict(t=10, b=10))
    st.plotly_chart(fig_ctrl, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Tabella dettaglio fatture (ultime 100, filtrata dalla sidebar)
# ------------------------------------------------------------------
st.subheader("📋 Dettaglio Fatture")

# Aggrega a livello testata (una riga per documento)
testate = (
    f.groupby(["anno", "serie", "numdoc", "tipo_doc", "data_doc", "cliente_nome"])["importo"]
    .sum()
    .reset_index()
    .sort_values("data_doc", ascending=False)
)
testate.columns = ["Anno", "Serie", "Num. Doc", "Tipo", "Data", "Cliente", "Importo (€)"]
testate["Tipo"] = testate["Tipo"].map({"A": "Fattura", "N": "Nota credito"}).fillna(testate["Tipo"])
testate["Data"] = testate["Data"].dt.date

st.caption(f"{len(testate):,} documenti trovati — mostro i primi 100")
st.dataframe(
    testate.head(100).reset_index(drop=True),
    use_container_width=True,
    column_config={
        "Importo (€)": st.column_config.NumberColumn(format="€ %.2f"),
    },
)
