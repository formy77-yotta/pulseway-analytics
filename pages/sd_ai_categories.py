"""
sd_ai_categories.py — Dashboard analisi AI categorizzazione ticket (Gemini / tickets_ai).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

from ai_dashboard_loaders import load_ai_ticket_data, load_queue_config_sidebar
from config import DATABASE_URL
from ticket_detail_dialog import issues_nonempty, show_ticket_detail, alert_level_display


# ------------------------------------------------------------------
# Caricamento dati
# ------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_ticket_notes(ticket_id: int) -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    df = pd.read_sql(
        text(
            """
            SELECT note_direction, details_clean, created_on
            FROM ticket_notes
            WHERE ticket_id = :tid
            ORDER BY created_on
            """
        ),
        engine,
        params={"tid": ticket_id},
    )
    if "created_on" in df.columns:
        df["created_on"] = pd.to_datetime(df["created_on"], utc=True, errors="coerce")
    return df


def explode_pattern_tags(series: pd.Series) -> pd.Series:
    out: list[str] = []
    for v in series.dropna():
        if isinstance(v, list):
            out.extend(str(x).strip() for x in v if str(x).strip())
        elif isinstance(v, str):
            s = v.strip("{}")
            for part in s.split(","):
                t = part.strip().strip('"').strip("'")
                if t:
                    out.append(t)
    return pd.Series(out, dtype=object)


# ------------------------------------------------------------------
# UI
# ------------------------------------------------------------------

st.title("🏷️ AI — Categorizzazione ticket")
st.caption("Analisi da tabella `tickets_ai` (Gemini) confrontata con dati operatore.")

try:
    df_all = load_ai_ticket_data()
except Exception as e:
    st.error(f"❌ Errore connessione DB: {e}")
    st.stop()

if df_all.empty:
    st.info(
        "Nessun ticket analizzato dall’AI. "
        "Esegui `python analyze_tickets.py` dopo aver popolato i ticket nel database."
    )
    st.stop()

# ------------------------------------------------------------------
# Sidebar — Filtri globali
# ------------------------------------------------------------------
st.sidebar.header("🔍 Filtri")

df_queue = load_queue_config_sidebar()
code_escluse = df_queue[df_queue["includi_analisi"] == False]["queue_name"].astype(str).tolist()  # noqa: E712
if code_escluse:
    st.sidebar.caption(f"⚠️ Code escluse: {', '.join(code_escluse)}")

min_d = df_all["open_date"].min()
max_d = df_all["open_date"].max()
min_date = min_d.date() if pd.notna(min_d) else pd.Timestamp.now().date()
max_date = max_d.date() if pd.notna(max_d) else pd.Timestamp.now().date()

date_range = st.sidebar.date_input(
    "Periodo analisi",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

accounts = ["Tutti"] + sorted(df_all["account_name"].dropna().unique().tolist())
sel_acc = st.sidebar.selectbox("Cliente", accounts)

techs = ["Tutti"] + sorted(df_all["assignee_name"].dropna().unique().tolist())
sel_tech = st.sidebar.selectbox("Tecnico", techs)

solo_rec = st.sidebar.checkbox("Solo ricorrenti", value=False)


def apply_sidebar_filters(src: pd.DataFrame) -> pd.DataFrame:
    f = src.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        f = f[
            (f["open_date"].dt.date >= date_range[0])
            & (f["open_date"].dt.date <= date_range[1])
        ]
    elif hasattr(date_range, "year"):
        f = f[f["open_date"].dt.date == date_range]
    if sel_acc != "Tutti":
        f = f[f["account_name"] == sel_acc]
    if sel_tech != "Tutti":
        f = f[f["assignee_name"] == sel_tech]
    if solo_rec:
        f = f[f["ai_is_recurring"] == True]  # noqa: E712
    return f


f = apply_sidebar_filters(df_all)

if f.empty:
    st.warning("Nessun ticket con i filtri selezionati.")
    st.stop()

_issue_cols = [
    "security_issues",
    "quality_issues",
    "communication_issues",
    "technical_issues",
    "process_issues",
]
for c in _issue_cols:
    if c not in f.columns:
        f[c] = None
if "alert_level" not in f.columns:
    f["alert_level"] = "ok"
else:
    f = f.copy()
    f["alert_level"] = f["alert_level"].fillna("ok")
if "has_sensitive_data" not in f.columns:
    f["has_sensitive_data"] = False

# ------------------------------------------------------------------
# 1. KPI
# ------------------------------------------------------------------
st.subheader("📊 KPI")
k1, k2, k3, k4, k5 = st.columns(5)
n = len(f)
match_ok = (f["category_match"] == True).sum()  # noqa: E712
rec_n = (f["ai_is_recurring"] == True).sum()  # noqa: E712
pct_match = round(100.0 * match_ok / n, 1) if n else 0.0
pct_rec = round(100.0 * rec_n / n, 1) if n else 0.0
mean_res = f["ai_resolution_quality"].mean()
mean_comm = f["ai_communication_quality"].mean()

k1.metric("Ticket analizzati", f"{n:,}")
k2.metric("% categoria corretta", f"{pct_match}%")
k3.metric("% ticket ricorrenti", f"{pct_rec}%")
k4.metric(
    "Media qualità risoluzione",
    f"{mean_res:.2f}" if pd.notna(mean_res) else "N/D",
)
k5.metric(
    "Media qualità comunicazione",
    f"{mean_comm:.2f}" if pd.notna(mean_comm) else "N/D",
)

st.subheader("🚨 Alert e Problemi Rilevati")


def _count_issue(col: str) -> int:
    if col not in f.columns:
        return 0
    return int(f[col].apply(issues_nonempty).sum())


z1, z2, z3, z4, z5 = st.columns(5)
z1.metric("🔴 Sicurezza", _count_issue("security_issues"))
z2.metric("🟡 Qualità", _count_issue("quality_issues"))
z3.metric("🟠 Comunicazione", _count_issue("communication_issues"))
z4.metric("🔵 Tecnico", _count_issue("technical_issues"))
z5.metric("⚫ Processo", _count_issue("process_issues"))

st.caption(
    "Conteggi: ticket con almeno un elemento negli array di problemi (nel filtro corrente)."
)

_sk = (
    f["alert_level"]
    .astype(str)
    .str.lower()
    .map({"critical": 0, "warning": 1, "ok": 2})
    .fillna(2)
)
alert_tbl = f.assign(_sk=_sk).sort_values("_sk").drop(columns=["_sk"])
alert_display = alert_tbl[
    ["ticket_number", "title", "account_name", "alert_level"]
].copy()
alert_display["alert_level"] = (
    alert_display["alert_level"].fillna("ok").astype(str).apply(alert_level_display)
)
st.dataframe(alert_display, use_container_width=True, hide_index=True)

st.divider()

# ------------------------------------------------------------------
# 2. Categorie AI vs Operatore
# ------------------------------------------------------------------
st.subheader("🔀 Categorie AI vs Operatore")

op_counts = (
    f["issue_type_name"].fillna("(non classificato)").value_counts().head(15).reset_index()
)
op_counts.columns = ["categoria", "n"]
op_counts["fonte"] = "Operatore"

ai_counts = f["ai_category"].fillna("(manca)").value_counts().head(15).reset_index()
ai_counts.columns = ["categoria", "n"]
ai_counts["fonte"] = "AI"

fig_cat = make_subplots(
    rows=1,
    cols=2,
    subplot_titles=("Categorie operatore (issue_type)", "Categorie AI"),
    horizontal_spacing=0.12,
)
fig_cat.add_trace(
    go.Bar(
        y=op_counts["categoria"][::-1],
        x=op_counts["n"][::-1],
        orientation="h",
        name="Operatore",
        marker_color="#636efa",
    ),
    row=1,
    col=1,
)
fig_cat.add_trace(
    go.Bar(
        y=ai_counts["categoria"][::-1],
        x=ai_counts["n"][::-1],
        orientation="h",
        name="AI",
        marker_color="#00cc96",
    ),
    row=1,
    col=2,
)
fig_cat.update_layout(height=420, showlegend=False, margin=dict(t=40, b=20))
st.plotly_chart(fig_cat, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# 3. Pattern ricorrenti
# ------------------------------------------------------------------
st.subheader("🔁 Pattern ricorrenti")

rec_df = f[f["ai_is_recurring"] == True].copy()  # noqa: E712
if rec_df.empty:
    st.caption("Nessun ticket ricorrente nei filtri correnti.")
else:
    st.markdown("**Ticket ricorrenti per categoria AI**")
    rec_list = rec_df.sort_values(["ai_category", "ticket_number"])[
        [
            "ai_category",
            "ticket_number",
            "title",
            "account_name",
            "assignee_name",
            "ai_root_cause",
        ]
    ]
    st.dataframe(rec_list, use_container_width=True, hide_index=True)

    rec_by_cat = (
        rec_df.groupby("ai_category", dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
    )
    fig_rec = px.bar(
        rec_by_cat,
        x="ai_category",
        y="n",
        labels={"ai_category": "Categoria AI", "n": "N. ticket ricorrenti"},
        color="n",
        color_continuous_scale="Reds",
    )
    fig_rec.update_layout(height=360, xaxis_tickangle=-35, margin=dict(b=120))
    st.plotly_chart(fig_rec, use_container_width=True)

root_counts = (
    f["ai_root_cause"]
    .dropna()
    .astype(str)
    .str.strip()
    .replace("", pd.NA)
    .dropna()
    .value_counts()
    .head(10)
    .reset_index()
)
root_counts.columns = ["ai_root_cause", "n"]
if not root_counts.empty:
    st.markdown("**Top 10 cause radice**")
    fig_root = px.bar(
        root_counts,
        x="n",
        y="ai_root_cause",
        orientation="h",
        labels={"n": "Frequenza", "ai_root_cause": "Causa radice"},
    )
    fig_root.update_layout(height=400, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig_root, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# 4. Qualità assistenza
# ------------------------------------------------------------------
st.subheader("⭐ Qualità assistenza")

tech_q = (
    f[f["assignee_name"].notna()]
    .groupby("assignee_name", dropna=False)
    .agg(
        n=("id", "count"),
        ai_resolution_quality=("ai_resolution_quality", "mean"),
        ai_communication_quality=("ai_communication_quality", "mean"),
    )
    .reset_index()
)
tech_q = tech_q.dropna(subset=["ai_resolution_quality", "ai_communication_quality"])

if not tech_q.empty:
    fig_sc = px.scatter(
        tech_q,
        x="ai_resolution_quality",
        y="ai_communication_quality",
        size="n",
        hover_name="assignee_name",
        size_max=55,
        labels={
            "ai_resolution_quality": "Media qualità risoluzione",
            "ai_communication_quality": "Media qualità comunicazione",
            "n": "N. ticket",
        },
        title="Un punto per tecnico (dimensione = numero ticket)",
    )
    fig_sc.update_layout(height=480)
    st.plotly_chart(fig_sc, use_container_width=True)
else:
    st.caption("Dati insufficienti per lo scatter tecnici.")

hm = f[f["assignee_name"].notna() & f["ai_resolution_quality"].notna()].copy()
if not hm.empty:
    pivot = hm.pivot_table(
        index="assignee_name",
        columns="mese",
        values="ai_resolution_quality",
        aggfunc="mean",
    )
    fig_hm = px.imshow(
        pivot,
        labels=dict(x="Mese", y="Tecnico", color="Media qualità risoluzione"),
        aspect="auto",
        color_continuous_scale="Viridis",
    )
    fig_hm.update_layout(height=max(320, 24 * len(pivot.index)))
    st.plotly_chart(fig_hm, use_container_width=True)

low_q = f[
    f["ai_resolution_quality"].notna() & (f["ai_resolution_quality"] <= 2)
].copy()
st.markdown("**Ticket con bassa qualità risoluzione (≤ 2)**")
if low_q.empty:
    st.caption("Nessuno nel filtro corrente.")
else:
    hl = st.columns([0.9, 2.2, 1.2, 1.0, 0.6, 1.4, 0.35])
    hl[0].markdown("**Ticket**")
    hl[1].markdown("**Titolo**")
    hl[2].markdown("**Tecnico**")
    hl[3].markdown("**Cliente**")
    hl[4].markdown("**Q. ris.**")
    hl[5].markdown("**Sommario AI**")
    hl[6].markdown("**Vedi**")

    for idx, row in low_q.iterrows():
        tid = int(row["id"])
        cols = st.columns([0.9, 2.2, 1.2, 1.0, 0.6, 1.4, 0.35])
        cols[0].write(row.get("ticket_number") or "")
        cols[1].write(str(row.get("title") or "")[:50])
        cols[2].write(str(row.get("assignee_name") or ""))
        cols[3].write(str(row.get("account_name") or ""))
        rq = row.get("ai_resolution_quality")
        cols[4].write(str(int(rq)) if pd.notna(rq) else "—")
        cols[5].write(str(row.get("ai_summary") or "")[:60])
        if cols[6].button("👁️", key=f"lowq_detail_{tid}_{idx}", help="Dettaglio ticket"):
            show_ticket_detail(tid)

st.divider()

# ------------------------------------------------------------------
# 5. Tag e urgenza
# ------------------------------------------------------------------
st.subheader("🏷️ Tag e urgenza")

tags_s = explode_pattern_tags(f["ai_pattern_tags"])
if not tags_s.empty:
    tag_counts = tags_s.value_counts().head(40).reset_index()
    tag_counts.columns = ["tag", "n"]
    fig_tags = px.bar(
        tag_counts.sort_values("n"),
        x="n",
        y="tag",
        orientation="h",
        labels={"n": "Frequenza", "tag": "Pattern tag"},
        title="Tag AI più frequenti",
    )
    fig_tags.update_layout(height=min(900, 200 + 18 * len(tag_counts)), yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig_tags, use_container_width=True)
else:
    st.caption("Nessun pattern tag disponibile.")

urgent_open = f[(f["ai_urgency_score"] == 5) & (~f["chiuso"])]
st.markdown("**Ticket urgenza 5 non ancora risolti**")
if urgent_open.empty:
    st.caption("Nessuno.")
else:
    uh = st.columns([0.9, 2.0, 1.1, 1.0, 1.0, 0.9, 1.0, 0.35])
    uh[0].markdown("**Ticket**")
    uh[1].markdown("**Titolo**")
    uh[2].markdown("**Cliente**")
    uh[3].markdown("**Tecnico**")
    uh[4].markdown("**Stato**")
    uh[5].markdown("**Apertura**")
    uh[6].markdown("**Sommario**")
    uh[7].markdown("**Vedi**")

    for idx, row in urgent_open.iterrows():
        tid = int(row["id"])
        od = row.get("open_date")
        od_s = (
            pd.to_datetime(od, utc=True, errors="coerce").strftime("%Y-%m-%d")
            if pd.notna(od)
            else "—"
        )
        cols = st.columns([0.9, 2.0, 1.1, 1.0, 1.0, 0.9, 1.0, 0.35])
        cols[0].write(row.get("ticket_number") or "")
        cols[1].write(str(row.get("title") or "")[:50])
        cols[2].write(str(row.get("account_name") or ""))
        cols[3].write(str(row.get("assignee_name") or ""))
        cols[4].write(str(row.get("status_name") or ""))
        cols[5].write(od_s)
        cols[6].write(str(row.get("ai_summary") or "")[:40])
        if cols[7].button("👁️", key=f"urg_detail_{tid}_{idx}", help="Dettaglio ticket"):
            show_ticket_detail(tid)

st.divider()

# ------------------------------------------------------------------
# 6. Dettaglio ticket
# ------------------------------------------------------------------
st.subheader("📋 Dettaglio ticket")

with st.expander("Filtri dettaglio e tabella completa", expanded=True):
    d_cat = ["Tutte"] + sorted(f["ai_category"].dropna().unique().tolist())
    d_tech = ["Tutti"] + sorted(f["assignee_name"].dropna().unique().tolist())
    d_acc = ["Tutti"] + sorted(f["account_name"].dropna().unique().tolist())
    rec_opt = ("Tutti", "Sì", "No")

    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        sf_cat = st.selectbox("Categoria AI", d_cat, key="det_cat")
    with fc2:
        sf_tech = st.selectbox("Tecnico", d_tech, key="det_tech")
    with fc3:
        sf_acc = st.selectbox("Cliente", d_acc, key="det_acc")
    with fc4:
        sf_rec = st.selectbox("Ricorrente", rec_opt, key="det_rec")

    detail = f.copy()
    if sf_cat != "Tutte":
        detail = detail[detail["ai_category"] == sf_cat]
    if sf_tech != "Tutti":
        detail = detail[detail["assignee_name"] == sf_tech]
    if sf_acc != "Tutti":
        detail = detail[detail["account_name"] == sf_acc]
    if sf_rec == "Sì":
        detail = detail[detail["ai_is_recurring"] == True]  # noqa: E712
    elif sf_rec == "No":
        detail = detail[detail["ai_is_recurring"] == False]  # noqa: E712

    ai_cols = [
        "ai_category",
        "ai_subcategory",
        "ai_confidence",
        "category_match",
        "category_note",
        "ai_root_cause",
        "ai_is_recurring",
        "ai_pattern_tags",
        "ai_urgency_score",
        "ai_resolution_quality",
        "ai_communication_quality",
        "ai_resolution_clear",
        "ai_quality_notes",
        "ai_summary",
        "ai_suggested_action",
        "analyzed_at",
    ]
    base_cols = [
        "ticket_number",
        "title",
        "account_name",
        "assignee_name",
        "issue_type_name",
        "status_name",
        "open_date",
        "completed_date",
    ]
    show_cols = base_cols + [c for c in ai_cols if c in detail.columns]

    st.dataframe(
        detail[show_cols],
        use_container_width=True,
        hide_index=True,
    )

    if detail.empty:
        st.caption("Nessun ticket con i filtri dettaglio.")
    else:
        labels = (
            detail["ticket_number"].astype(str)
            + " — "
            + detail["title"].fillna("").str.slice(0, 80)
        )
        opt_map = dict(zip(labels, detail["id"].tolist()))
        picked_label = st.selectbox("Apri note ticket", list(opt_map.keys()))
        tid = opt_map[picked_label]
        row = detail[detail["id"] == tid].iloc[0]
        st.markdown(f"**Titolo:** {row.get('title', '')}")
        st.markdown(f"**Sommario AI:** {row.get('ai_summary', '')}")
        notes_df = load_ticket_notes(int(tid))
        if notes_df.empty:
            st.caption("Nessuna nota per questo ticket.")
        else:
            st.markdown("**Note (interne / cliente)**")
            st.dataframe(notes_df, use_container_width=True)
