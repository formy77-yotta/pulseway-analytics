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
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
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
            cp.categoria   AS contropartita_cat,
            cp.tipo        AS contropartita_tipo
        FROM fact_vendite v
        LEFT JOIN dim_clienti       c  ON c.nts_id  = v.cliente_id
        LEFT JOIN dim_contropartite cp ON cp.codice  = v.contropartita
    """, engine)

    vendite["data_doc"] = pd.to_datetime(vendite["data_doc"], errors="coerce")

    # Normalizza NULL → "Non classificato" per categoria e tipo
    vendite["contropartita_cat"]  = vendite["contropartita_cat"].fillna("Non classificato")
    vendite["contropartita_tipo"] = vendite["contropartita_tipo"].fillna("ALTRO")
    vendite["contropartita_desc"] = vendite["contropartita_desc"].fillna(
        vendite["contropartita"].astype(str)
    )

    contropartite = pd.read_sql("SELECT * FROM dim_contropartite", engine)

    return vendite, contropartite


try:
    df_raw, df_ctrl = load_data()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

if df_raw.empty:
    st.info("Nessun dato disponibile in fact_vendite.")
    st.stop()

anno_corrente   = date.today().year
anno_precedente = anno_corrente - 1

# ------------------------------------------------------------------
# SIDEBAR — Filtri
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

anni_disponibili = sorted(df_raw["anno"].dropna().unique().tolist(), reverse=True)
sel_anni = st.sidebar.multiselect(
    "Anno", anni_disponibili,
    default=[anno_corrente] if anno_corrente in anni_disponibili else anni_disponibili[:1],
)

clienti_lista = ["Tutti"] + sorted(df_raw["cliente_nome"].dropna().unique().tolist())
sel_cliente = st.sidebar.selectbox("Cliente", clienti_lista)

tipi_ctrl = sorted(df_raw["contropartita_tipo"].unique().tolist())
# Default: solo RICAVO — esclude acquisti/costi/finanziario come in Power BI
tipi_default = [t for t in tipi_ctrl if t == "RICAVO"]
if not tipi_default:          # fallback se RICAVO non esiste ancora nel DB
    tipi_default = tipi_ctrl
sel_tipi_ctrl = st.sidebar.multiselect("Tipo contropartita", tipi_ctrl, default=tipi_default)

# Le categorie disponibili cambiano in base al tipo selezionato
cat_lista = sorted(
    df_raw[df_raw["contropartita_tipo"].isin(sel_tipi_ctrl)]["contropartita_cat"]
    .unique().tolist()
)
sel_cat = st.sidebar.multiselect("Categoria", cat_lista, default=cat_lista)

tipo_doc_map = {"Tutti": None, "Solo fatture": "A", "Solo note credito": "N"}
sel_tipo_label = st.sidebar.selectbox("Tipo documento", list(tipo_doc_map.keys()))
sel_tipo = tipo_doc_map[sel_tipo_label]

# Applica filtri
f = df_raw.copy()
if sel_anni:
    f = f[f["anno"].isin(sel_anni)]
if sel_cliente != "Tutti":
    f = f[f["cliente_nome"] == sel_cliente]
if set(sel_tipi_ctrl) != set(tipi_ctrl):
    f = f[f["contropartita_tipo"].isin(sel_tipi_ctrl)]
if set(sel_cat) != set(cat_lista):
    f = f[f["contropartita_cat"].isin(sel_cat)]
if sel_tipo:
    f = f[f["tipo_doc"] == sel_tipo]

# ------------------------------------------------------------------
# KPI  — sempre su df_raw senza filtri sidebar
# ------------------------------------------------------------------
st.subheader("📊 KPI Principali")

oggi        = date.today()
cutoff_prec = oggi.replace(year=anno_precedente)

ytd  = df_raw[df_raw["anno"] == anno_corrente]["importo"].sum()
prec = df_raw[
    (df_raw["anno"] == anno_precedente) &
    (df_raw["data_doc"].dt.date <= cutoff_prec)
]["importo"].sum()
yoy  = round((ytd - prec) / abs(prec) * 100, 1) if prec else None

n_fatture    = (
    df_raw[df_raw["anno"] == anno_corrente]
    .drop_duplicates(subset=["serie", "numdoc"])
    .shape[0]
)
importo_medio = round(ytd / n_fatture, 2) if n_fatture else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric(f"Fatturato {anno_corrente}",
          f"€ {ytd:,.0f}".replace(",", "."))
c2.metric(f"{anno_precedente} (fino al {cutoff_prec.strftime('%d/%m')})",
          f"€ {prec:,.0f}".replace(",", "."))
c3.metric("Variazione YoY",
          f"{yoy:+.1f}%" if yoy is not None else "N/D",
          delta=f"{yoy:+.1f}%" if yoy is not None else None)
c4.metric("Fatture emesse", f"{n_fatture:,}".replace(",", "."))
c5.metric("Importo medio per fattura",
          f"€ {importo_medio:,.2f}".replace(",", "."))

st.divider()

# ------------------------------------------------------------------
# Trend mensile + Cumulato
# ------------------------------------------------------------------
mese_labels = {
    1:"Gen", 2:"Feb", 3:"Mar", 4:"Apr", 5:"Mag", 6:"Giu",
    7:"Lug", 8:"Ago", 9:"Set", 10:"Ott", 11:"Nov", 12:"Dic",
}

f_trend = f.copy()
f_trend["mese"]     = f_trend["data_doc"].dt.month
f_trend["anno_str"] = f_trend["anno"].astype(str)

trend = (
    f_trend.groupby(["anno_str", "mese"])["importo"]
    .sum()
    .reset_index()
    .sort_values(["anno_str", "mese"])
)
trend["mese_label"]  = trend["mese"].map(mese_labels)
trend["importo_cum"] = trend.groupby("anno_str")["importo"].cumsum()

tab_mensile, tab_cumulato = st.tabs(["📅 Trend Mensile", "📈 Andamento Cumulato"])

with tab_mensile:
    fig_trend = px.line(
        trend, x="mese_label", y="importo", color="anno_str",
        markers=True,
        category_orders={"mese_label": list(mese_labels.values())},
        labels={"importo": "Fatturato (€)", "mese_label": "Mese", "anno_str": "Anno"},
    )
    fig_trend.update_layout(height=360, margin=dict(t=10, b=10))
    st.plotly_chart(fig_trend, use_container_width=True)

with tab_cumulato:
    fig_cum = px.line(
        trend, x="mese_label", y="importo_cum", color="anno_str",
        markers=True,
        category_orders={"mese_label": list(mese_labels.values())},
        labels={"importo_cum": "Fatturato cumulato (€)", "mese_label": "Mese", "anno_str": "Anno"},
    )
    fig_cum.update_layout(height=360, margin=dict(t=10, b=10))
    st.plotly_chart(fig_cum, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Fatturato per Categoria
# ------------------------------------------------------------------
st.subheader("🏷️ Composizione per Categoria")

anni_in_filtro = sorted(f["anno"].dropna().unique().tolist(), reverse=True)

if not anni_in_filtro:
    st.info("Nessun dato per gli anni selezionati.")
else:
    col_cat1, col_cat2 = st.columns(2)

    with col_cat1:
        # Stacked bar: categorie × anno
        f_cat = f.copy()
        f_cat["anno_str"] = f_cat["anno"].astype(str)
        cat_anno = (
            f_cat.groupby(["anno_str", "contropartita_cat"])["importo"]
            .sum()
            .reset_index()
        )
        fig_cat_bar = px.bar(
            cat_anno, x="anno_str", y="importo", color="contropartita_cat",
            barmode="stack",
            labels={"importo": "€", "anno_str": "Anno", "contropartita_cat": "Categoria"},
            title="Composizione per anno",
        )
        fig_cat_bar.update_layout(height=360, margin=dict(t=30, b=10))
        st.plotly_chart(fig_cat_bar, use_container_width=True)

    with col_cat2:
        # Donut sull'anno più recente nel filtro
        anno_ref = anni_in_filtro[0]
        cat_pie = (
            f[f["anno"] == anno_ref]
            .groupby("contropartita_cat")["importo"]
            .sum()
            .reset_index()
            .sort_values("importo", ascending=False)
        )
        cat_pie.columns = ["Categoria", "Fatturato"]
        fig_cat_pie = px.pie(
            cat_pie, values="Fatturato", names="Categoria",
            hole=0.45,
            title=f"Composizione — {anno_ref}",
        )
        fig_cat_pie.update_layout(height=360, margin=dict(t=30, b=10))
        st.plotly_chart(fig_cat_pie, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Top 15 Clienti + dettaglio per voce  (un solo anno)
# ------------------------------------------------------------------
st.subheader("🏢 Top 15 Clienti & Voci")

if not anni_in_filtro:
    st.info("Nessun dato per gli anni selezionati.")
else:
    anno_sel = st.radio(
        "Anno di riferimento",
        options=anni_in_filtro,
        index=0,
        horizontal=True,
        key="radio_anno_grafici",
    )

    f_anno = f[f["anno"] == anno_sel]

    col_a, col_b = st.columns([3, 2])

    with col_a:
        top_cli = (
            f_anno.groupby("cliente_nome")["importo"]
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
            title=f"Top 15 Clienti — {anno_sel}",
        )
        fig_cli.update_layout(height=420, margin=dict(t=30, b=10),
                              yaxis=dict(autorange="reversed"))
        fig_cli.update_traces(texttemplate="€ %{x:,.0f}", textposition="outside")
        st.plotly_chart(fig_cli, use_container_width=True)

    with col_b:
        tab_cat2, tab_desc = st.tabs(["Per categoria", "Per voce"])

        with tab_cat2:
            by_cat = (
                f_anno.groupby("contropartita_cat")["importo"]
                .sum()
                .reset_index()
                .sort_values("importo", ascending=False)
            )
            by_cat.columns = ["Categoria", "Fatturato"]
            fig_cat2 = px.pie(
                by_cat, values="Fatturato", names="Categoria",
                hole=0.45,
                title=f"Per Categoria — {anno_sel}",
            )
            fig_cat2.update_layout(height=390, margin=dict(t=30, b=10))
            st.plotly_chart(fig_cat2, use_container_width=True)

        with tab_desc:
            by_desc = (
                f_anno.groupby("contropartita_desc")["importo"]
                .sum()
                .reset_index()
                .sort_values("importo", ascending=False)
            )
            by_desc.columns = ["Voce", "Fatturato"]
            fig_desc = px.bar(
                by_desc, x="Fatturato", y="Voce", orientation="h",
                color="Fatturato", color_continuous_scale="Blues",
                title=f"Per Voce — {anno_sel}",
                labels={"Fatturato": "€"},
            )
            fig_desc.update_layout(height=390, margin=dict(t=30, b=10),
                                   yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_desc, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Tabella dettaglio fatture
# ------------------------------------------------------------------
st.subheader("📋 Dettaglio Fatture")

fc1, fc2, fc3 = st.columns([2, 2, 1])

data_min = f["data_doc"].min().date()
data_max = f["data_doc"].max().date()

with fc1:
    det_date = st.date_input(
        "Periodo", value=(data_min, data_max),
        min_value=data_min, max_value=data_max,
        key="det_date",
    )
with fc2:
    det_tipo = st.multiselect(
        "Tipo documento",
        options=["Fattura", "Nota credito"],
        default=["Fattura", "Nota credito"],
        key="det_tipo",
    )
with fc3:
    det_limit = st.selectbox(
        "Mostra", options=[100, 250, 500, 1000], index=0, key="det_limit",
    )

testate = (
    f.groupby(["anno", "serie", "numdoc", "tipo_doc", "data_doc", "cliente_nome"])["importo"]
    .sum()
    .reset_index()
    .sort_values("data_doc", ascending=False)
)
testate.columns = ["Anno", "Serie", "Num. Doc", "Tipo", "Data", "Cliente", "Importo (€)"]
testate["Tipo"] = testate["Tipo"].map({"A": "Fattura", "N": "Nota credito"}).fillna(testate["Tipo"])
testate["Data"] = pd.to_datetime(testate["Data"]).dt.date

if len(det_date) == 2:
    testate = testate[(testate["Data"] >= det_date[0]) & (testate["Data"] <= det_date[1])]
if det_tipo:
    testate = testate[testate["Tipo"].isin(det_tipo)]

st.caption(f"{len(testate):,} documenti — mostro gli ultimi {det_limit}")
st.dataframe(
    testate.head(det_limit).reset_index(drop=True),
    use_container_width=True,
    column_config={
        "Importo (€)": st.column_config.NumberColumn(format="€ %.2f"),
        "Data":        st.column_config.DateColumn(format="DD/MM/YYYY"),
    },
)
