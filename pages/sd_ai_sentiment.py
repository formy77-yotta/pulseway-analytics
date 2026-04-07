"""
sd_ai_sentiment.py — Discrepanze categoria operatore vs AI (sentiment / allineamento).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ai_dashboard_loaders import load_ai_ticket_data, load_queue_config_sidebar
from ticket_detail_dialog import alert_level_display, show_ticket_detail

st.title("💬 AI Sentiment")
st.caption(
    "Confronto tra classificazione operatore (Pulseway) e categoria AI: discrepanze, "
    "livello di alert e dettaglio ticket."
)

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
    out = src.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        out = out[
            (out["open_date"].dt.date >= date_range[0])
            & (out["open_date"].dt.date <= date_range[1])
        ]
    elif hasattr(date_range, "year"):
        out = out[out["open_date"].dt.date == date_range]
    if sel_acc != "Tutti":
        out = out[out["account_name"] == sel_acc]
    if sel_tech != "Tutti":
        out = out[out["assignee_name"] == sel_tech]
    if solo_rec:
        out = out[out["ai_is_recurring"] == True]  # noqa: E712
    return out


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

st.subheader("🔀 Discrepanze categoria operatore vs AI")
solo_discrepanze = st.checkbox("Mostra solo discrepanze", value=False)

disc_base = f.copy()
if solo_discrepanze:
    disc_view = disc_base[disc_base["category_match"] == False].copy()  # noqa: E712
else:
    disc_view = disc_base.copy()

if disc_view.empty:
    st.caption(
        "Nessun ticket da mostrare (solo discrepanze: nessuna discrepanza nei filtri correnti)."
        if solo_discrepanze
        else "Nessun ticket nei filtri correnti."
    )
else:
    h = st.columns([0.85, 0.95, 1.0, 1.75, 0.95, 0.45, 0.75, 0.55, 0.35])
    h[0].markdown("**Ticket**")
    h[1].markdown("**Apertura**")
    h[2].markdown("**Cliente**")
    h[3].markdown("**Titolo**")
    h[4].markdown("**Cat. AI**")
    h[5].markdown("**Match**")
    h[6].markdown("**Alert**")
    h[7].markdown("**Conf.**")
    h[8].markdown("**Vedi**")

    for idx, row in disc_view.iterrows():
        tid = int(row["id"])
        od = row.get("open_date")
        if pd.notna(od):
            od_s = pd.to_datetime(od, utc=True, errors="coerce").strftime("%Y-%m-%d %H:%M")
        else:
            od_s = "—"
        conf = row.get("ai_confidence")
        conf_s = f"{float(conf):.0%}" if pd.notna(conf) else "—"
        cm = row.get("category_match")
        if pd.isna(cm):
            match_s = "—"
        elif bool(cm):
            match_s = "✅"
        else:
            match_s = "⚠️"
        title_s = str(row.get("title") or "")[:50]
        if solo_discrepanze and pd.notna(conf) and float(conf) > 0.8:
            title_s = f"🔴 {title_s}"

        al_disp = alert_level_display(row.get("alert_level"))

        cols = st.columns([0.85, 0.95, 1.0, 1.75, 0.95, 0.45, 0.75, 0.55, 0.35])
        cols[0].write(row.get("ticket_number") or "")
        cols[1].write(od_s)
        cols[2].write(str(row.get("account_name") or ""))
        cols[3].write(title_s)
        cols[4].write(str(row.get("ai_category") or ""))
        cols[5].write(match_s)
        cols[6].markdown(al_disp)
        cols[7].write(conf_s)
        if cols[8].button("👁️", key=f"sent_disc_{tid}_{idx}", help="Dettaglio ticket"):
            show_ticket_detail(tid)
