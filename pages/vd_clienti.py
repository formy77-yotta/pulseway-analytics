"""
vd_clienti.py — Dashboard Clienti.
Analisi clienti: fatturato, ticket, SLA, trend, mappa province.
Join: dim_clienti.pulseway_id = tickets.account_id
"""

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from config import DATABASE_URL

st.title("🏢 Clienti")

# ------------------------------------------------------------------
# Caricamento dati
# ------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    engine = create_engine(DATABASE_URL)

    clienti = pd.read_sql(
        "SELECT nts_id, pulseway_id, nome, citta, provincia, tipo, attivo FROM dim_clienti",
        engine,
    )

    vendite = pd.read_sql(
        """
        SELECT v.cliente_id, v.anno, v.data_doc, v.importo, v.serie, v.numdoc
        FROM fact_vendite v
        JOIN dim_contropartite cp ON cp.codice = v.contropartita
        WHERE v.cliente_id IS NOT NULL
          AND cp.tipo = 'RICAVO'
        """,
        engine,
    )
    vendite["data_doc"] = pd.to_datetime(vendite["data_doc"], errors="coerce")

    tickets = pd.read_sql(
        """
        SELECT account_id, open_date, completed_date, has_met_sla
        FROM tickets
        WHERE account_id IS NOT NULL
        """,
        engine,
    )
    tickets["open_date"]      = pd.to_datetime(tickets["open_date"],      utc=True, errors="coerce")
    tickets["completed_date"] = pd.to_datetime(tickets["completed_date"], utc=True, errors="coerce")

    return clienti, vendite, tickets


try:
    df_clienti, df_vendite, df_tickets = load_data()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

if df_clienti.empty:
    st.info("Nessun dato disponibile in dim_clienti.")
    st.stop()

anno_corrente   = date.today().year
anno_precedente = anno_corrente - 1
cutoff_prec     = date.today().replace(year=anno_precedente)

# ------------------------------------------------------------------
# SIDEBAR — Filtri
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

anni_disponibili = sorted(df_vendite["anno"].dropna().unique().tolist(), reverse=True)
sel_anni = st.sidebar.multiselect(
    "Anno fatturato",
    anni_disponibili,
    default=[anno_corrente] if anno_corrente in anni_disponibili else anni_disponibili[:1],
)

province_disponibili = sorted(df_clienti["provincia"].dropna().unique().tolist())
sel_province = st.sidebar.multiselect("Provincia", province_disponibili, default=[])

solo_con_fatturato = st.sidebar.checkbox("Solo clienti con fatturato", value=True)
solo_con_ticket    = st.sidebar.checkbox("Solo clienti con ticket",    value=False)

# ------------------------------------------------------------------
# Aggregazioni vendite
# ------------------------------------------------------------------
vv = df_vendite.copy()
if sel_anni:
    vv = vv[vv["anno"].isin(sel_anni)]

# Fatturato totale per cliente (periodo filtrato)
fat_tot = (
    vv.groupby("cliente_id")
    .agg(fatturato=("importo", "sum"), n_fatture=("numdoc", "nunique"))
    .reset_index()
    .rename(columns={"cliente_id": "nts_id"})
)

# Fatturato YTD anno corrente
fat_ytd = (
    df_vendite[df_vendite["anno"] == anno_corrente]
    .groupby("cliente_id")["importo"].sum()
    .reset_index()
    .rename(columns={"cliente_id": "nts_id", "importo": "fat_ytd"})
)

# Fatturato anno precedente stesso periodo (per YoY)
fat_prec = (
    df_vendite[
        (df_vendite["anno"] == anno_precedente) &
        (df_vendite["data_doc"].dt.date <= cutoff_prec)
    ]
    .groupby("cliente_id")["importo"].sum()
    .reset_index()
    .rename(columns={"cliente_id": "nts_id", "importo": "fat_prec"})
)

# ------------------------------------------------------------------
# Aggregazioni ticket
# ------------------------------------------------------------------
tt = df_tickets.copy()

ticket_tot = (
    tt.groupby("account_id")
    .agg(
        n_ticket_tot=("account_id", "count"),
        n_ticket_aperti=("completed_date", lambda x: x.isna().sum()),
        ultimo_ticket=("open_date", "max"),
        sla_ok=("has_met_sla", lambda x: (x == 1).sum()),
        sla_tot=("has_met_sla", lambda x: x.notna().sum()),
    )
    .reset_index()
    .rename(columns={"account_id": "pulseway_id"})
)
ticket_tot["sla_pct"] = (
    ticket_tot["sla_ok"] / ticket_tot["sla_tot"].replace(0, float("nan")) * 100
).round(1)
ticket_tot["ultimo_ticket"] = ticket_tot["ultimo_ticket"].dt.tz_localize(None)

# ------------------------------------------------------------------
# Build tabella clienti (master join)
# ------------------------------------------------------------------
df = (
    df_clienti
    .merge(fat_tot,    on="nts_id",      how="left")
    .merge(fat_ytd,    on="nts_id",      how="left")
    .merge(fat_prec,   on="nts_id",      how="left")
    .merge(ticket_tot, on="pulseway_id", how="left")
)

df["fatturato"]      = df["fatturato"].fillna(0)
df["fat_ytd"]        = df["fat_ytd"].fillna(0)
df["fat_prec"]       = df["fat_prec"].fillna(0)
df["n_fatture"]      = df["n_fatture"].fillna(0).astype(int)
df["n_ticket_tot"]   = df["n_ticket_tot"].fillna(0).astype(int)
df["n_ticket_aperti"]= df["n_ticket_aperti"].fillna(0).astype(int)
df["var_yoy"]        = (
    (df["fat_ytd"] - df["fat_prec"]) / df["fat_prec"].replace(0, float("nan")) * 100
).round(1)

# Applica filtri sidebar
if sel_province:
    df = df[df["provincia"].isin(sel_province)]
if solo_con_fatturato:
    df = df[df["fatturato"] > 0]
if solo_con_ticket:
    df = df[df["n_ticket_tot"] > 0]

df = df.sort_values("fatturato", ascending=False)

# ------------------------------------------------------------------
# KPI
# ------------------------------------------------------------------
st.subheader("📊 KPI Principali")

n_clienti_attivi = (df["fatturato"] > 0).sum()
fat_medio        = df[df["fatturato"] > 0]["fatturato"].mean()
top_row          = df.iloc[0] if not df.empty else None
n_con_aperti     = (df["n_ticket_aperti"] > 0).sum()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Clienti con fatturato", f"{n_clienti_attivi:,}")
k2.metric(
    "Fatturato medio per cliente",
    f"€ {fat_medio:,.0f}".replace(",", ".") if pd.notna(fat_medio) else "N/D",
)
k3.metric(
    "Cliente top",
    top_row["nome"] if top_row is not None else "N/D",
    delta=f"€ {top_row['fatturato']:,.0f}".replace(",", ".") if top_row is not None else None,
    delta_color="off",
)
k4.metric("Clienti con ticket aperti", f"{n_con_aperti:,}")

st.divider()

# ------------------------------------------------------------------
# Tabella clienti principale
# ------------------------------------------------------------------
st.subheader("📋 Tabella Clienti")

col_anno_corr = f"Fatt. {anno_corrente}"
col_anno_prec = f"Fatt. {anno_precedente} (fino {cutoff_prec.strftime('%d/%m')})"

cols_display = [
    "nome", "citta", "fatturato", "n_fatture",
    "fat_ytd", "fat_prec", "var_yoy",
    "n_ticket_tot", "n_ticket_aperti", "ultimo_ticket", "sla_pct",
]
tbl = df[cols_display].copy()
tbl.columns = [
    "Cliente", "Città", "Fatturato tot.", "N° Fatture",
    col_anno_corr, col_anno_prec, "Var% YoY",
    "Ticket tot.", "Ticket aperti", "Ultimo ticket", "SLA %",
]
tbl["Ultimo ticket"] = pd.to_datetime(tbl["Ultimo ticket"]).dt.date


def _color_rows(row):
    if row["Ticket aperti"] > 5:
        return ["background-color: #ffd6d6"] * len(row)
    if pd.notna(row["SLA %"]) and row["SLA %"] > 90:
        return ["background-color: #d6f5d6"] * len(row)
    return [""] * len(row)


styled = tbl.reset_index(drop=True).style.apply(_color_rows, axis=1).format({
    "Fatturato tot.": "€ {:,.0f}",
    col_anno_corr:    "€ {:,.0f}",
    col_anno_prec:    "€ {:,.0f}",
    "Var% YoY":       "{:+.1f}%",
    "SLA %":          "{:.1f}%",
}, na_rep="—")

st.dataframe(styled, use_container_width=True, height=420)

leg1, leg2, _ = st.columns([1, 1, 4])
leg1.markdown(
    '<div style="background:#ffd6d6;padding:4px 10px;border-radius:4px;font-size:0.85em">'
    '🔴 Ticket aperti &gt; 5</div>',
    unsafe_allow_html=True,
)
leg2.markdown(
    '<div style="background:#d6f5d6;padding:4px 10px;border-radius:4px;font-size:0.85em">'
    '🟢 SLA rispettato &gt; 90%</div>',
    unsafe_allow_html=True,
)

st.divider()

# ------------------------------------------------------------------
# Scatter: Fatturato vs Ticket
# ------------------------------------------------------------------
st.subheader("🔵 Fatturato vs Ticket")

scatter_df = df[df["fatturato"] > 0].copy()
scatter_df["n_ticket_aperti_vis"] = scatter_df["n_ticket_aperti"].clip(lower=1)  # min size bolla

if scatter_df.empty:
    st.info("Nessun dato per il grafico.")
else:
    fig_sc = px.scatter(
        scatter_df,
        x="fatturato",
        y="n_ticket_tot",
        size="n_ticket_aperti_vis",
        color="sla_pct",
        hover_name="nome",
        hover_data={
            "fatturato":          ":,.0f",
            "n_ticket_tot":       True,
            "n_ticket_aperti":    True,
            "sla_pct":            ":.1f",
            "n_ticket_aperti_vis": False,
        },
        color_continuous_scale="RdYlGn",
        range_color=[0, 100],
        labels={
            "fatturato":    "Fatturato (€)",
            "n_ticket_tot": "N° Ticket totali",
            "sla_pct":      "SLA %",
        },
        title="Clienti: fatturato vs volume ticket (bolla = ticket aperti, colore = SLA%)",
    )
    fig_sc.update_layout(height=460, margin=dict(t=40, b=10))
    st.plotly_chart(fig_sc, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Trend mensile top 5 clienti
# ------------------------------------------------------------------
st.subheader("📅 Trend Mensile — Top 5 Clienti")

top5_ids = df.head(5)["nts_id"].tolist()
top5_nomi = df.head(5).set_index("nts_id")["nome"].to_dict()

vv_top5 = df_vendite[
    df_vendite["cliente_id"].isin(top5_ids) &
    df_vendite["anno"].isin(sel_anni if sel_anni else anni_disponibili)
].copy()

if vv_top5.empty:
    st.info("Nessun dato di vendita per i top 5 clienti nel periodo selezionato.")
else:
    vv_top5["mese"]        = vv_top5["data_doc"].dt.to_period("M").astype(str)
    vv_top5["cliente_nome"]= vv_top5["cliente_id"].map(top5_nomi)

    trend5 = (
        vv_top5.groupby(["mese", "cliente_nome"])["importo"]
        .sum()
        .reset_index()
        .sort_values("mese")
    )

    fig_trend = px.line(
        trend5, x="mese", y="importo", color="cliente_nome",
        markers=True,
        labels={"importo": "Fatturato (€)", "mese": "Mese", "cliente_nome": "Cliente"},
    )
    fig_trend.update_layout(height=360, margin=dict(t=10, b=10))
    fig_trend.update_xaxes(tickangle=30)
    st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Mappa per provincia
# ------------------------------------------------------------------
st.subheader("🗺️ Distribuzione per Provincia")

prov_fat = (
    df[df["fatturato"] > 0]
    .groupby("provincia")
    .agg(fatturato=("fatturato", "sum"), n_clienti=("nts_id", "count"))
    .reset_index()
    .sort_values("fatturato", ascending=False)
    .head(20)
)

if prov_fat.empty:
    st.info("Nessun dato per provincia.")
else:
    col_p1, col_p2 = st.columns(2)

    with col_p1:
        fig_pf = px.bar(
            prov_fat, x="fatturato", y="provincia", orientation="h",
            color="fatturato", color_continuous_scale="Blues",
            labels={"fatturato": "Fatturato (€)", "provincia": "Provincia"},
            title="Fatturato per provincia",
        )
        fig_pf.update_layout(height=420, margin=dict(t=30, b=10), yaxis=dict(autorange="reversed"))
        fig_pf.update_traces(texttemplate="€ %{x:,.0f}", textposition="outside")
        st.plotly_chart(fig_pf, use_container_width=True)

    with col_p2:
        fig_pc = px.bar(
            prov_fat.sort_values("n_clienti", ascending=False),
            x="n_clienti", y="provincia", orientation="h",
            color="n_clienti", color_continuous_scale="Purples",
            labels={"n_clienti": "N° Clienti", "provincia": "Provincia"},
            title="N° clienti per provincia",
        )
        fig_pc.update_layout(height=420, margin=dict(t=30, b=10), yaxis=dict(autorange="reversed"))
        fig_pc.update_traces(texttemplate="%{x}", textposition="outside")
        st.plotly_chart(fig_pc, use_container_width=True)
