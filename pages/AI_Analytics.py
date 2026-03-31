"""
AI_Analytics.py — Pagina AI Pulseway Analytics (multi-pagina Streamlit).
Analisi anomalie e pattern con Claude API.

Avvio: python -m streamlit run app.py (la sidebar apre questa pagina)
"""

import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import requests
from config import DATABASE_URL, DASHBOARD_PASSWORD, ANTHROPIC_API_KEY

from auth import check_auth
check_auth()

st.title("🤖 Pulseway PSA — AI Analytics")
st.caption("Analisi anomalie e pattern nascosti powered by Claude AI")

# ------------------------------------------------------------------
# Caricamento dati
# ------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM tickets", engine)

    date_cols = ["open_date", "completed_date", "first_response_actual_time",
                 "resolution_actual_time", "created_on"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    df["mese"]  = df["open_date"].dt.to_period("M").astype(str)
    df["chiuso"] = df["completed_date"].notna()
    df["settimana"] = df["open_date"].dt.to_period("W").astype(str)

    return df


try:
    df = load_data()
except Exception as e:
    st.error(f"❌ Errore DB: {e}")
    st.stop()

if df.empty:
    st.warning("Nessun dato. Esegui prima `python sync.py`.")
    st.stop()

# ------------------------------------------------------------------
# Sidebar filtri
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

min_date = df["open_date"].min().date()
max_date = df["open_date"].max().date()
date_range = st.sidebar.date_input("Periodo", value=(min_date, max_date),
                                    min_value=min_date, max_value=max_date)

accounts  = ["Tutti"] + sorted(df["account_name"].dropna().unique().tolist())
sel_acc   = st.sidebar.selectbox("Cliente", accounts)

f = df.copy()
if len(date_range) == 2:
    f = f[(f["open_date"].dt.date >= date_range[0]) & (f["open_date"].dt.date <= date_range[1])]
if sel_acc != "Tutti":
    f = f[f["account_name"] == sel_acc]

# ------------------------------------------------------------------
# Funzione chiamata Claude API
# ------------------------------------------------------------------

def ask_claude(prompt: str, context_data: str) -> str:
    """Chiama Claude API e restituisce l'analisi testuale."""
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,          # ← aggiungi questa
                "anthropic-version": "2023-06-01",        # ← e questa
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 1000,
                "system": (
                    "Sei un analista esperto di service desk IT. "
                    "Analizzi dati di ticket di supporto e identifichi anomalie, "
                    "pattern nascosti e insight azionabili. "
                    "Rispondi in italiano, in modo conciso e diretto. "
                    "Usa bullet points. Sii specifico sui numeri quando disponibili."
                ),
                "messages": [
                    {"role": "user", "content": f"{prompt}\n\nDATI:\n{context_data}"}
                ],
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"]
    except Exception as e:
        return f"⚠️ Errore API Claude: {e}"


# ------------------------------------------------------------------
# SEZIONE 1 — Anomalie tempi di chiusura
# ------------------------------------------------------------------
st.subheader("⏱️ Anomalie nei Tempi di Chiusura")

col1, col2 = st.columns([2, 1])

chiusi = f[f["chiuso"] & f["biz_hours_resolution"].notna() & (f["biz_hours_resolution"] > 0)].copy()

with col1:
    if not chiusi.empty:
        # Box plot per categoria
        top_cats = chiusi["issue_type_name"].value_counts().head(8).index
        box_df = chiusi[chiusi["issue_type_name"].isin(top_cats)]

        # Calcola media + 2 std per ogni categoria (anomalie)
        stats = chiusi.groupby("issue_type_name")["biz_hours_resolution"].agg(["mean","std"]).reset_index()
        stats["soglia_anomalia"] = stats["mean"] + 2 * stats["std"].fillna(0)

        fig = px.box(
            box_df,
            x="issue_type_name",
            y="biz_hours_resolution",
            title="Distribuzione ore lavorative chiusura per categoria",
            labels={"biz_hours_resolution": "Ore lavorative", "issue_type_name": "Categoria"},
            color="issue_type_name",
        )
        fig.update_layout(height=380, showlegend=False, margin=dict(t=30, b=80))
        fig.update_xaxes(tickangle=30)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nessun ticket chiuso con dati sufficienti.")

with col2:
    st.markdown("**🤖 Analisi AI**")
    if st.button("Analizza anomalie tempi", key="btn_tempi"):
        if not chiusi.empty:
            stats_str = chiusi.groupby("issue_type_name")["biz_hours_resolution"].agg(
                count="count", media="mean", mediana="median",
                p90=lambda x: x.quantile(0.9)
            ).round(1).to_string()

            # Ticket anomali (> media + 2std)
            anomali = []
            for cat, grp in chiusi.groupby("issue_type_name"):
                soglia = grp["biz_hours_resolution"].mean() + 2 * grp["biz_hours_resolution"].std()
                outliers = grp[grp["biz_hours_resolution"] > soglia]
                if len(outliers) > 0:
                    anomali.append(f"{cat}: {len(outliers)} ticket anomali (soglia {soglia:.0f}h)")

            anomali_str = "\n".join(anomali) if anomali else "Nessuna anomalia rilevata"

            with st.spinner("Claude sta analizzando..."):
                risposta = ask_claude(
                    "Analizza questi dati sui tempi di chiusura ticket per categoria. "
                    "Identifica anomalie, categorie problematiche e suggerisci azioni concrete.",
                    f"STATISTICHE PER CATEGORIA:\n{stats_str}\n\nTICKET ANOMALI (>2 std):\n{anomali_str}"
                )
            st.markdown(risposta)
        else:
            st.warning("Nessun dato disponibile.")

st.divider()

# ------------------------------------------------------------------
# SEZIONE 2 — Clienti a rischio
# ------------------------------------------------------------------
st.subheader("🏢 Clienti a Rischio (Trend Peggiorativo)")

col3, col4 = st.columns([2, 1])

with col3:
    # Ticket per cliente per mese
    trend_cli = (
        f.groupby(["account_name", "mese"])
        .size()
        .reset_index(name="n_ticket")
    )
    top_acc_list = f["account_name"].value_counts().head(8).index
    trend_cli_top = trend_cli[trend_cli["account_name"].isin(top_acc_list)]

    fig2 = px.line(
        trend_cli_top, x="mese", y="n_ticket", color="account_name",
        title="Trend mensile ticket per cliente (top 8)",
        labels={"n_ticket": "N° Ticket", "mese": "Mese", "account_name": "Cliente"},
        markers=True,
    )
    fig2.update_layout(height=380, margin=dict(t=30, b=10))
    fig2.update_xaxes(tickangle=30)
    st.plotly_chart(fig2, use_container_width=True)

with col4:
    st.markdown("**🤖 Analisi AI**")
    if st.button("Identifica clienti a rischio", key="btn_clienti"):
        # Calcola variazione ultimi 2 mesi vs 2 mesi precedenti
        mesi = sorted(f["mese"].dropna().unique())
        if len(mesi) >= 4:
            ultimi2  = mesi[-2:]
            prec2    = mesi[-4:-2]
            rec  = f[f["mese"].isin(ultimi2)].groupby("account_name").size().reset_index(name="recenti")
            prec = f[f["mese"].isin(prec2)].groupby("account_name").size().reset_index(name="precedenti")
            comp = rec.merge(prec, on="account_name", how="outer").fillna(0)
            comp["variazione_pct"] = ((comp["recenti"] - comp["precedenti"]) / (comp["precedenti"] + 1) * 100).round(1)
            comp = comp.sort_values("variazione_pct", ascending=False)
            top10 = comp.head(10).to_string(index=False)

            with st.spinner("Claude sta analizzando..."):
                risposta = ask_claude(
                    "Analizza il trend ticket per cliente. "
                    "Identifica clienti con peggioramento significativo e possibile rischio. "
                    "Suggerisci azioni proattive per i clienti più critici.",
                    f"VARIAZIONE TICKET (ultimi 2 mesi vs 2 mesi precedenti):\n{top10}"
                )
            st.markdown(risposta)
        else:
            st.info("Servono almeno 4 mesi di dati.")

st.divider()

# ------------------------------------------------------------------
# SEZIONE 3 — Pattern anomali per tecnico
# ------------------------------------------------------------------
st.subheader("👤 Pattern Anomali per Tecnico")

col5, col6 = st.columns([2, 1])

tech_stats = (
    f[f["assignee_name"].notna() & (f["assignee_name"].str.strip() != "")]
    .groupby("assignee_name")
    .agg(
        n_ticket=("id", "count"),
        pct_chiusi=("chiuso", "mean"),
        med_ore_chiusura=("biz_hours_resolution", "median"),
        med_ore_risposta=("biz_hours_first_response", "median"),
        sla_ok=("has_met_sla", lambda x: (x==1).sum()),
        sla_tot=("has_met_sla", lambda x: x.notna().sum()),
    )
    .reset_index()
)
tech_stats["pct_sla"] = (tech_stats["sla_ok"] / tech_stats["sla_tot"].replace(0,1) * 100).round(1)
tech_stats["pct_chiusi"] = (tech_stats["pct_chiusi"] * 100).round(1)

with col5:
    fig3 = px.scatter(
        tech_stats[tech_stats["n_ticket"] >= 5],
        x="med_ore_chiusura",
        y="pct_sla",
        size="n_ticket",
        color="assignee_name",
        hover_data=["n_ticket", "pct_chiusi", "med_ore_risposta"],
        title="Tecnici: ore chiusura vs % SLA (bolla = volume ticket)",
        labels={"med_ore_chiusura": "Mediana ore chiusura", "pct_sla": "% SLA rispettato",
                "assignee_name": "Tecnico"},
    )
    fig3.update_layout(height=400, margin=dict(t=30, b=10))
    st.plotly_chart(fig3, use_container_width=True)

with col6:
    st.markdown("**🤖 Analisi AI**")
    if st.button("Analizza pattern tecnici", key="btn_tecnici"):
        tech_str = tech_stats[tech_stats["n_ticket"] >= 5][
            ["assignee_name","n_ticket","pct_chiusi","med_ore_chiusura","med_ore_risposta","pct_sla"]
        ].round(1).to_string(index=False)

        with st.spinner("Claude sta analizzando..."):
            risposta = ask_claude(
                "Analizza le performance dei tecnici. "
                "Identifica pattern anomali: chi chiude troppo velocemente (possibile chiusura frettolosa), "
                "chi ha tempi molto alti, chi ha bassa % SLA rispetto agli altri. "
                "Suggerisci azioni di coaching o redistribuzione del carico.",
                f"STATISTICHE TECNICI:\n{tech_str}"
            )
        st.markdown(risposta)

st.divider()

# ------------------------------------------------------------------
# SEZIONE 4 — Problemi ricorrenti non categorizzati
# ------------------------------------------------------------------
st.subheader("🏷️ Problemi Ricorrenti Non Categorizzati")

col7, col8 = st.columns([2, 1])

with col7:
    # Ticket senza categoria o con categoria generica
    no_cat = f[f["issue_type_name"].isna() | f["issue_type_name"].isin(["Other","Altro","General"])]
    cat_vuote = f["issue_type_name"].isna().sum()
    totale = len(f)

    fig4 = px.bar(
        f["issue_type_name"].fillna("⚠️ Non classificato").value_counts().head(15).reset_index(),
        x="count", y="issue_type_name", orientation="h",
        title=f"Distribuzione categorie ({cat_vuote} ticket non classificati su {totale})",
        labels={"count": "N° Ticket", "issue_type_name": "Categoria"},
        color="count", color_continuous_scale="Blues",
    )
    fig4.update_layout(height=400, margin=dict(t=30, b=10), yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig4, use_container_width=True)

with col8:
    st.markdown("**🤖 Analisi AI**")
    if st.button("Analizza problemi ricorrenti", key="btn_ricorrenti"):
        # Campiona titoli non categorizzati
        titoli = no_cat["title"].dropna().sample(min(50, len(no_cat)), random_state=42).tolist()
        titoli_str = "\n".join(f"- {t}" for t in titoli)

        cat_dist = f["issue_type_name"].fillna("Non classificato").value_counts().head(15).to_string()

        with st.spinner("Claude sta analizzando..."):
            risposta = ask_claude(
                "Analizza questi titoli di ticket non categorizzati o genericamente categorizzati. "
                "Identifica pattern ricorrenti, proponi nuove categorie o sottocategorie, "
                "e suggerisci quali ticket potrebbero essere automatizzati o prevenuti.",
                f"TITOLI TICKET NON CATEGORIZZATI (campione):\n{titoli_str}\n\n"
                f"DISTRIBUZIONE ATTUALE CATEGORIE:\n{cat_dist}"
            )
        st.markdown(risposta)

st.divider()

# ------------------------------------------------------------------
# SEZIONE 5 — Analisi Contatto Diretto e Fuori Orario
# ------------------------------------------------------------------
st.subheader("📞 Analisi Contatto Diretto & Fuori Orario")

col9, col10 = st.columns([2, 1])

has_custom = "custom_contatto_diretto" in f.columns and "custom_fuori_orario" in f.columns

with col9:
    if has_custom:
        # Distribuzione per cliente
        cf_df = f[f["custom_contatto_diretto"].notna() | f["custom_fuori_orario"].notna()].copy()

        by_acc = cf_df.groupby("account_name").agg(
            contatto_diretto=("custom_contatto_diretto", lambda x: (x=="Yes").sum()),
            fuori_orario=("custom_fuori_orario", lambda x: (x=="Yes").sum()),
            totale=("id", "count"),
        ).reset_index()
        by_acc = by_acc[(by_acc["contatto_diretto"] > 0) | (by_acc["fuori_orario"] > 0)]
        by_acc = by_acc.sort_values("contatto_diretto", ascending=False).head(15)

        if not by_acc.empty:
            fig5 = px.bar(
                by_acc, x="account_name",
                y=["contatto_diretto", "fuori_orario"],
                barmode="group",
                title="Contatto diretto e richieste fuori orario per cliente",
                labels={"value": "N° Ticket", "account_name": "Cliente", "variable": ""},
                color_discrete_map={"contatto_diretto": "#636efa", "fuori_orario": "#ef553b"},
            )
            fig5.update_layout(height=400, margin=dict(t=30, b=80))
            fig5.update_xaxes(tickangle=30)
            st.plotly_chart(fig5, use_container_width=True)

            # Trend mensile
            trend_cf = cf_df.groupby("mese").agg(
                contatto_diretto=("custom_contatto_diretto", lambda x: (x=="Yes").sum()),
                fuori_orario=("custom_fuori_orario", lambda x: (x=="Yes").sum()),
            ).reset_index()

            fig6 = px.line(
                trend_cf, x="mese",
                y=["contatto_diretto", "fuori_orario"],
                title="Trend mensile contatto diretto e fuori orario",
                markers=True,
                color_discrete_map={"contatto_diretto": "#636efa", "fuori_orario": "#ef553b"},
            )
            fig6.update_layout(height=300, margin=dict(t=30, b=10))
            st.plotly_chart(fig6, use_container_width=True)
        else:
            st.info("Ancora nessun ticket con questi campi valorizzati.")
    else:
        st.info("Campi custom non ancora disponibili nel DB. Esegui `python migrate_business_hours.py` e `python sync.py`.")

with col10:
    st.markdown("**🤖 Analisi AI**")
    if st.button("Analizza contatti diretti e fuori orario", key="btn_custom"):
        if has_custom:
            cf_df = f[f["custom_contatto_diretto"].notna() | f["custom_fuori_orario"].notna()].copy()

            by_cat = cf_df.groupby("issue_type_name").agg(
                contatto_diretto=("custom_contatto_diretto", lambda x: (x=="Yes").sum()),
                fuori_orario=("custom_fuori_orario", lambda x: (x=="Yes").sum()),
                totale=("id", "count"),
            ).reset_index()
            by_cat["pct_contatto"] = (by_cat["contatto_diretto"] / by_cat["totale"] * 100).round(1)
            by_cat["pct_fuori"]    = (by_cat["fuori_orario"]    / by_cat["totale"] * 100).round(1)

            by_acc = cf_df.groupby("account_name").agg(
                contatto_diretto=("custom_contatto_diretto", lambda x: (x=="Yes").sum()),
                fuori_orario=("custom_fuori_orario", lambda x: (x=="Yes").sum()),
                totale=("id", "count"),
            ).reset_index().sort_values("contatto_diretto", ascending=False).head(10)

            with st.spinner("Claude sta analizzando..."):
                risposta = ask_claude(
                    "Analizza i pattern di contatto diretto e richieste fuori orario. "
                    "Identifica quali clienti e categorie generano più richieste fuori orario o contatti diretti. "
                    "Suggerisci azioni per ridurre questi pattern (es. formazione, documentazione, SLA dedicati).",
                    f"PER CATEGORIA:\n{by_cat.to_string(index=False)}\n\n"
                    f"TOP CLIENTI:\n{by_acc.to_string(index=False)}"
                )
            st.markdown(risposta)
        else:
            st.info("Campi custom non ancora disponibili.")

st.divider()

# ------------------------------------------------------------------
# SEZIONE 6 — Report AI Completo
# ------------------------------------------------------------------
st.subheader("📋 Report AI Completo")

if st.button("🤖 Genera report completo", key="btn_report", type="primary"):
    # Prepara sommario dati
    totale = len(f)
    chiusi_n = f["chiuso"].sum()
    med_chiusura = f["biz_hours_resolution"].median()
    sla_pct = round((f["has_met_sla"]==1).sum() / totale * 100, 1) if totale else 0

    top_problemi = f["issue_type_name"].value_counts().head(5).to_string()
    top_clienti  = f["account_name"].value_counts().head(5).to_string()

    tech_perf = tech_stats[tech_stats["n_ticket"] >= 5][
        ["assignee_name","n_ticket","med_ore_chiusura","pct_sla"]
    ].round(1).to_string(index=False)

    mesi_trend = f.groupby("mese").size().tail(6).to_string()

    with st.spinner("Claude sta generando il report completo... (può richiedere 15-20 secondi)"):
        risposta = ask_claude(
            "Genera un report esecutivo completo sull'andamento del service desk. "
            "Includi: sintesi generale, anomalie critiche rilevate, top 3 aree di miglioramento, "
            "raccomandazioni prioritarie. Tono professionale, per un manager IT.",
            f"PERIODO: {date_range[0] if len(date_range)==2 else 'tutto'} → {date_range[1] if len(date_range)==2 else ''}\n"
            f"TOTALE TICKET: {totale} | CHIUSI: {chiusi_n} | SLA OK: {sla_pct}%\n"
            f"MEDIANA ORE CHIUSURA (lavorative): {med_chiusura:.1f}h\n\n"
            f"TOP CATEGORIE:\n{top_problemi}\n\n"
            f"TOP CLIENTI:\n{top_clienti}\n\n"
            f"PERFORMANCE TECNICI:\n{tech_perf}\n\n"
            f"TREND MENSILE:\n{mesi_trend}"
        )

    st.markdown(risposta)

    # Opzione download
    st.download_button(
        "📥 Scarica report",
        data=risposta,
        file_name=f"report_ai_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
        mime="text/plain",
    )

st.caption(f"🕒 Dati aggiornati al: {df['synced_at'].max() if 'synced_at' in df.columns else 'N/D'} | Ticket nel DB: {len(df):,}")
