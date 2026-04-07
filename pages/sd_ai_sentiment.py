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


def _build_discrepanze_table_df(
    disc_view: pd.DataFrame, solo_discrepanze: bool
) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in disc_view.iterrows():
        od = row.get("open_date")
        if pd.notna(od):
            od_dt = pd.to_datetime(od, utc=True, errors="coerce")
        else:
            od_dt = pd.NaT
        conf = row.get("ai_confidence")
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
        conf_pct = float(conf) * 100.0 if pd.notna(conf) else None
        rows.append(
            {
                "Ticket": row.get("ticket_number") or "",
                "Apertura": od_dt,
                "Cliente": str(row.get("account_name") or ""),
                "Titolo": title_s,
                "Cat. AI": str(row.get("ai_category") or ""),
                "Match": match_s,
                "Alert": alert_level_display(row.get("alert_level")),
                "Conf. %": conf_pct,
                "_ticket_id": int(row["id"]),
            }
        )
    return pd.DataFrame(rows)


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
    display_df = _build_discrepanze_table_df(disc_view, solo_discrepanze).reset_index(drop=True)
    visible_cols = [
        "Ticket",
        "Apertura",
        "Cliente",
        "Titolo",
        "Cat. AI",
        "Match",
        "Alert",
        "Conf. %",
    ]
    st.caption(
        "Tabella interattiva: clicca sull’intestazione di una colonna per ordinare. "
        "Seleziona una riga e usa il pulsante per aprire il dettaglio."
    )
    df_state = st.dataframe(
        display_df,
        column_order=visible_cols,
        column_config={
            "Ticket": st.column_config.TextColumn("Ticket", width="small"),
            "Apertura": st.column_config.DatetimeColumn(
                "Apertura", format="YYYY-MM-DD HH:mm", timezone="UTC"
            ),
            "Cliente": st.column_config.TextColumn("Cliente", width="medium"),
            "Titolo": st.column_config.TextColumn("Titolo", width="large"),
            "Cat. AI": st.column_config.TextColumn("Cat. AI", width="medium"),
            "Match": st.column_config.TextColumn("Match", width="small"),
            "Alert": st.column_config.TextColumn("Alert", width="small"),
            "Conf. %": st.column_config.NumberColumn(
                "Conf. %",
                format="%.0f %%",
                min_value=0.0,
                max_value=100.0,
            ),
        },
        use_container_width=True,
        hide_index=True,
        height=480,
        on_select="rerun",
        selection_mode="single-row",
        key="sentiment_discrepanze_table",
    )
    sel_rows = df_state.selection.rows
    detail_col1, detail_col2 = st.columns([1, 4])
    with detail_col1:
        open_disabled = len(sel_rows) == 0
        if st.button(
            "👁️ Dettaglio ticket",
            disabled=open_disabled,
            help="Seleziona una riga nella tabella, poi clicca qui.",
        ):
            ridx = sel_rows[0]
            show_ticket_detail(int(display_df.iloc[ridx]["_ticket_id"]))
