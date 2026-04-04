"""
sd_dashboard.py — Dashboard Pulseway Analytics con Streamlit + PostgreSQL.
"""

import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from config import DATABASE_URL, DASHBOARD_PASSWORD, ANTHROPIC_API_KEY

st.title("🎫 Pulseway PSA — Analisi Ticket")

# ------------------------------------------------------------------
# Caricamento dati da PostgreSQL
# ------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_tickets() -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM tickets", engine)

    date_cols = [
        "open_date", "completed_date", "due_date",
        "first_response_actual_time", "first_response_target_time",
        "resolution_actual_time", "resolution_target_time",
        "created_on", "modified_on",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Campi derivati
    df["ore_chiusura"] = (
        (df["completed_date"] - df["open_date"]).dt.total_seconds() / 3600
    ).round(1)
    df["ore_prima_risposta"] = (df["actual_first_response_min"] / 60).round(1)
    df["ore_risoluzione"]    = (df["actual_resolution_min"] / 60).round(1)
    df["mese"]               = df["open_date"].dt.to_period("M").astype(str)
    df["sla_rispettato"]     = df["has_met_sla"].map({1: "✅ Rispettato", 0: "❌ Violato"})
    df["chiuso"]             = df["completed_date"].notna()

    return df


try:
    df = load_tickets()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

if df.empty:
    st.warning("Nessun dato. Esegui prima `python sync.py`.")
    st.stop()

# ------------------------------------------------------------------
# SIDEBAR — Filtri
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

min_date = df["open_date"].min().date()
max_date = df["open_date"].max().date()
date_range = st.sidebar.date_input("Periodo", value=(min_date, max_date),
                                    min_value=min_date, max_value=max_date)

accounts  = ["Tutti"] + sorted(df["account_name"].dropna().unique().tolist())
sel_acc   = st.sidebar.selectbox("Cliente", accounts)

assignees  = ["Tutti"] + sorted(df["assignee_name"].dropna().unique().tolist())
sel_tech   = st.sidebar.selectbox("Tecnico", assignees)

priorities = sorted(df["priority_name"].dropna().unique().tolist())
sel_prio   = st.sidebar.multiselect("Priorità", priorities, default=[])

statuses   = sorted(df["status_name"].dropna().unique().tolist())
sel_status = st.sidebar.multiselect("Stato", statuses, default=[])

# Filtro coda
queues = sorted(df["queue_name"].dropna().unique().tolist())
sel_queue = st.sidebar.multiselect("Coda", queues, default=[])

# Applica filtri
f = df.copy()
if len(date_range) == 2:
    f = f[(f["open_date"].dt.date >= date_range[0]) & (f["open_date"].dt.date <= date_range[1])]
if sel_acc   != "Tutti":  f = f[f["account_name"]  == sel_acc]
if sel_tech  != "Tutti":  f = f[f["assignee_name"] == sel_tech]
if sel_prio:              f = f[f["priority_name"].isin(sel_prio)]
if sel_status:            f = f[f["status_name"].isin(sel_status)]
if sel_queue:             f = f[f["queue_name"].isin(sel_queue)]

# ------------------------------------------------------------------
# KPI
# ------------------------------------------------------------------
st.subheader("📊 KPI Principali")
c1, c2, c3, c4, c5 = st.columns(5)
total    = len(f)
chiusi   = int(f["chiuso"].sum())
aperti   = total - chiusi
med_ch   = f[f["ore_chiusura"] > 0]["ore_chiusura"].median()
sla_pct  = round((f["has_met_sla"] == 1).sum() / total * 100, 1) if total else 0

c1.metric("Totale",          f"{total:,}")
c2.metric("Aperti",          f"{aperti:,}")
c3.metric("Chiusi",          f"{chiusi:,}")
c4.metric("Mediana chiusura", f"{med_ch:.1f} h" if pd.notna(med_ch) else "N/D")
c5.metric("SLA rispettato",  f"{sla_pct}%")

st.divider()

# ------------------------------------------------------------------
# Trend mensile + Stato
# ------------------------------------------------------------------
ca, cb = st.columns([2, 1])
with ca:
    st.subheader("📈 Trend Mensile")
    trend = f.groupby("mese").size().reset_index(name="count")
    ct    = f[f["chiuso"]].groupby("mese").size().reset_index(name="chiusi")
    trend = trend.merge(ct, on="mese", how="left").fillna(0)
    trend["aperti"] = trend["count"] - trend["chiusi"].astype(int)
    fig = px.bar(trend, x="mese", y=["aperti","chiusi"], barmode="stack",
                 color_discrete_map={"aperti":"#ef553b","chiusi":"#00cc96"},
                 labels={"value":"Ticket","mese":"Mese","variable":""})
    fig.update_layout(height=320, margin=dict(t=10,b=10))
    st.plotly_chart(fig, use_container_width=True)

with cb:
    st.subheader("🔵 Per Stato")
    stato = f["status_name"].value_counts().reset_index()
    stato.columns = ["Stato","N"]
    fig2 = px.pie(stato, values="N", names="Stato", hole=0.4)
    fig2.update_layout(height=320, margin=dict(t=10,b=10))
    st.plotly_chart(fig2, use_container_width=True)

# ------------------------------------------------------------------
# Categoria + Priorità
# ------------------------------------------------------------------
cc, cd = st.columns(2)
with cc:
    st.subheader("🏷️ Per Categoria")
    cats = f["issue_type_name"].fillna("Non classificato").value_counts().head(15).reset_index()
    cats.columns = ["Categoria","N"]
    fig3 = px.bar(cats, x="N", y="Categoria", orientation="h",
                  color="N", color_continuous_scale="Blues")
    fig3.update_layout(height=400, margin=dict(t=10,b=10), yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig3, use_container_width=True)

with cd:
    st.subheader("⚡ Per Priorità")
    prio = f["priority_name"].value_counts().reset_index()
    prio.columns = ["Priorità","N"]
    cmap = {"Critical":"#d62728","High":"#ff7f0e","Medium":"#2ca02c","Low":"#1f77b4"}
    fig4 = px.bar(prio, x="Priorità", y="N", color="Priorità",
                  color_discrete_map=cmap)
    fig4.update_layout(height=400, margin=dict(t=10,b=10), showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)

# ------------------------------------------------------------------
# Analisi tecnici
# ------------------------------------------------------------------
st.subheader("👤 Analisi per Tecnico")
tech = (
    f[f["assignee_name"].notna()]
    .groupby("assignee_name")
    .agg(n=("id","count"),
         med_chiusura=("ore_chiusura","median"),
         med_risposta=("ore_prima_risposta","median"))
    .reset_index()
    .rename(columns={"assignee_name":"Tecnico"})
    .sort_values("n", ascending=False)
    .head(15)
)
ce, cf_ = st.columns(2)
with ce:
    fig5 = px.bar(tech, x="Tecnico", y="n", title="Carico per Tecnico",
                  labels={"n":"N° Ticket"})
    fig5.update_layout(height=360, margin=dict(t=30,b=80))
    fig5.update_xaxes(tickangle=30)
    st.plotly_chart(fig5, use_container_width=True)

with cf_:
    fig6 = px.bar(tech, x="Tecnico", y="med_chiusura",
                  title="Mediana Ore Chiusura",
                  labels={"med_chiusura":"Ore"},
                  color="med_chiusura", color_continuous_scale="RdYlGn_r")
    fig6.update_layout(height=360, margin=dict(t=30,b=80))
    fig6.update_xaxes(tickangle=30)
    st.plotly_chart(fig6, use_container_width=True)

# ------------------------------------------------------------------
# Top clienti + SLA
# ------------------------------------------------------------------
cg, ch = st.columns(2)
with cg:
    st.subheader("🏢 Top Clienti")
    top = f["account_name"].value_counts().head(15).reset_index()
    top.columns = ["Cliente","N"]
    fig7 = px.bar(top, x="N", y="Cliente", orientation="h",
                  color="N", color_continuous_scale="Purples")
    fig7.update_layout(height=400, margin=dict(t=10,b=10), yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig7, use_container_width=True)

with ch:
    st.subheader("🎯 SLA Rispettato vs Violato")
    sla_d = f[f["has_met_sla"].notna()]["sla_rispettato"].value_counts().reset_index()
    sla_d.columns = ["SLA","N"]
    fig8 = px.pie(sla_d, values="N", names="SLA", hole=0.4,
                  color="SLA", color_discrete_map={"✅ Rispettato":"#00cc96","❌ Violato":"#ef553b"})
    fig8.update_layout(height=400, margin=dict(t=10,b=10))
    st.plotly_chart(fig8, use_container_width=True)

# ------------------------------------------------------------------
# Contatto Diretto & Fuori Orario
# ------------------------------------------------------------------
if "custom_contatto_diretto" in f.columns and "custom_fuori_orario" in f.columns:
    st.subheader("📞 Contatto Diretto & Fuori Orario")
    ck, cl = st.columns(2)

    with ck:
        cd = f["custom_contatto_diretto"].value_counts().reset_index()
        cd.columns = ["Valore", "N"]
        fig_cd = px.pie(cd, values="N", names="Valore", hole=0.4,
                        title="Contatto Diretto",
                        color_discrete_sequence=["#636efa","#ef553b","#d3d3d3"])
        fig_cd.update_layout(height=320, margin=dict(t=30,b=10))
        st.plotly_chart(fig_cd, use_container_width=True)

    with cl:
        fo = f["custom_fuori_orario"].value_counts().reset_index()
        fo.columns = ["Valore", "N"]
        fig_fo = px.pie(fo, values="N", names="Valore", hole=0.4,
                        title="Richiesta Fuori Orario",
                        color_discrete_sequence=["#ef553b","#636efa","#d3d3d3"])
        fig_fo.update_layout(height=320, margin=dict(t=30,b=10))
        st.plotly_chart(fig_fo, use_container_width=True)

    # Barre per cliente
    by_acc_cf = f.groupby("account_name").agg(
        contatto_diretto=("custom_contatto_diretto", lambda x: (x=="Yes").sum()),
        fuori_orario=("custom_fuori_orario", lambda x: (x=="Yes").sum()),
    ).reset_index()
    by_acc_cf = by_acc_cf[
        (by_acc_cf["contatto_diretto"] > 0) | (by_acc_cf["fuori_orario"] > 0)
    ].sort_values("contatto_diretto", ascending=False).head(15)

    if not by_acc_cf.empty:
        fig_acc = px.bar(
            by_acc_cf, x="account_name",
            y=["contatto_diretto", "fuori_orario"],
            barmode="group",
            title="Contatto diretto e fuori orario per cliente",
            labels={"value":"N° Ticket","account_name":"Cliente","variable":""},
            color_discrete_map={"contatto_diretto":"#636efa","fuori_orario":"#ef553b"},
        )
        fig_acc.update_layout(height=360, margin=dict(t=30,b=80))
        fig_acc.update_xaxes(tickangle=30)
        st.plotly_chart(fig_acc, use_container_width=True)

    st.divider()

# ------------------------------------------------------------------
# Distribuzione tempi
# ------------------------------------------------------------------
st.subheader("⏱️ Distribuzione Tempi")
ci, cj = st.columns(2)
chiusi_df = f[f["ore_chiusura"].between(0, 720)]
risposta_df = f[f["ore_prima_risposta"].between(0, 48)]

with ci:
    fig9 = px.histogram(chiusi_df, x="ore_chiusura", nbins=40,
                        title="Ore dalla apertura alla chiusura",
                        color_discrete_sequence=["#636efa"])
    fig9.update_layout(height=320, margin=dict(t=30,b=10))
    st.plotly_chart(fig9, use_container_width=True)

with cj:
    fig10 = px.histogram(risposta_df, x="ore_prima_risposta", nbins=30,
                         title="Ore dalla apertura alla prima risposta",
                         color_discrete_sequence=["#00cc96"])
    fig10.update_layout(height=320, margin=dict(t=30,b=10))
    st.plotly_chart(fig10, use_container_width=True)

# ------------------------------------------------------------------
# Tabella raw
# ------------------------------------------------------------------
st.divider()
with st.expander("📋 Dati grezzi (primi 500)"):
    cols = ["ticket_number","title","account_name","assignee_name",
            "status_name","priority_name","issue_type_name","sub_issue_type_name",
            "open_date","completed_date","ore_chiusura","ore_prima_risposta","sla_rispettato"]
    cols = [c for c in cols if c in f.columns]
    st.dataframe(f[cols].head(500).reset_index(drop=True), use_container_width=True)

last_sync = df["synced_at"].max() if "synced_at" in df.columns else "N/D"
st.caption(f"🕒 Ultimo sync: {last_sync} | Ticket nel DB: {len(df):,}")
