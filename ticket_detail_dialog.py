"""Dialog Streamlit: dettaglio ticket + note + analisi AI (condiviso tra pagine)."""

from __future__ import annotations

import html
import re

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from config import DATABASE_URL


def issues_nonempty(val) -> bool:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    if isinstance(val, (list, tuple)):
        return len(val) > 0
    return False


def alert_level_display(level) -> str:
    a = str(level or "ok").strip().lower()
    if a == "critical":
        return "🔴 critical"
    if a == "warning":
        return "🟡 warning"
    return "🟢 ok"


@st.dialog("🎫 Dettaglio Ticket", width="large")
def show_ticket_detail(ticket_id: int) -> None:
    """Popup con dettaglio ticket e conversazione."""
    url = (DATABASE_URL or "").replace("postgres://", "postgresql://", 1)
    engine = create_engine(url)

    ticket_df = pd.read_sql(
        text(
            """
            SELECT
                t.*,
                ai.ai_category, ai.ai_subcategory, ai.ai_confidence,
                ai.ai_summary, ai.ai_root_cause, ai.ai_is_recurring,
                ai.ai_resolution_quality, ai.ai_communication_quality,
                ai.ai_quality_notes, ai.ai_suggested_action,
                ai.ai_pattern_tags, ai.ai_urgency_score,
                ai.category_match, ai.category_note,
                ai.security_issues, ai.quality_issues, ai.communication_issues,
                ai.technical_issues, ai.process_issues,
                ai.has_sensitive_data, ai.alert_level
            FROM tickets t
            LEFT JOIN tickets_ai ai ON ai.ticket_id = t.id
            WHERE t.id = :tid
            """
        ),
        engine,
        params={"tid": ticket_id},
    )
    if ticket_df.empty:
        st.error("Ticket non trovato.")
        return

    ticket = ticket_df.iloc[0]

    notes = pd.read_sql(
        text(
            """
            SELECT note_direction, details_clean, created_by_name, created_on
            FROM ticket_notes
            WHERE ticket_id = :tid
            ORDER BY created_on ASC
            """
        ),
        engine,
        params={"tid": ticket_id},
    )
    if "created_on" in notes.columns:
        notes["created_on"] = pd.to_datetime(notes["created_on"], utc=True, errors="coerce")

    col1, col2 = st.columns([3, 2])

    with col1:
        st.subheader(f"#{ticket['ticket_number']} — {ticket['title']}")

        c1, c2, c3 = st.columns(3)
        c1.metric("Stato", ticket.get("status_name") or "—")
        c2.metric("Priorità", ticket.get("priority_name") or "—")
        c3.metric("Cliente", ticket.get("account_name") or "—")

        c4, c5 = st.columns(2)
        c4.metric("Tecnico", ticket.get("assignee_name") or "Non assegnato")
        od = ticket.get("open_date")
        c5.metric("Apertura", str(od)[:10] if pd.notna(od) else "—")

        details_raw = ticket.get("details")
        if details_raw and str(details_raw).strip():
            with st.expander("📄 Descrizione", expanded=True):
                desc = re.sub(r"<[^>]+>", " ", str(details_raw))
                desc = re.sub(r"\s+", " ", desc).strip()
                st.write(desc[:1000])

        st.markdown("---")
        st.subheader("💬 Conversazione")

        if notes.empty:
            st.info("Nessuna nota disponibile per questo ticket.")
        else:
            for _, note in notes.iterrows():
                is_internal = note.get("note_direction") == "internal"
                author = html.escape(str(note.get("created_by_name") or "—"))
                created = str(note.get("created_on") or "")[:16]
                body = html.escape(str(note.get("details_clean") or "")).replace("\n", "<br>")

                if is_internal:
                    st.markdown(
                        f"""
                    <div style="background:#f0f0f0; padding:10px;
                                border-radius:8px; margin:5px 0;
                                border-left:4px solid #888;">
                    <small>🔒 <b>{author}</b> — {created}</small><br>
                    {body}
                    </div>
                    """,
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f"""
                    <div style="background:#e8f4fd; padding:10px;
                                border-radius:8px; margin:5px 0;
                                border-left:4px solid #1f77b4;">
                    <small>📧 <b>{author}</b> — {created}</small><br>
                    {body}
                    </div>
                    """,
                        unsafe_allow_html=True,
                    )

    with col2:
        st.subheader("🤖 Analisi AI")

        if pd.notna(ticket.get("ai_category")):
            if ticket.get("ai_summary"):
                st.info(f"📝 {ticket['ai_summary']}")

            match_icon = "✅" if ticket.get("category_match") else "⚠️"
            st.markdown(f"**Categoria operatore:** {ticket.get('issue_type_name') or '—'}")
            st.markdown(f"**Categoria AI:** {match_icon} {ticket.get('ai_category')}")
            if ticket.get("category_note"):
                st.caption(str(ticket["category_note"]))

            st.markdown("---")
            st.markdown("**Qualità servizio:**")

            res_q = ticket.get("ai_resolution_quality")
            com_q = ticket.get("ai_communication_quality")

            if pd.notna(res_q) and res_q is not None:
                rq = int(res_q)
                stars = "⭐" * max(0, min(5, rq))
                st.markdown(f"Risoluzione: {stars} ({rq}/5)")
            if pd.notna(com_q) and com_q is not None:
                cq = int(com_q)
                stars = "⭐" * max(0, min(5, cq))
                st.markdown(f"Comunicazione: {stars} ({cq}/5)")

            if ticket.get("ai_quality_notes"):
                st.caption(f"💬 {ticket['ai_quality_notes']}")

            st.markdown("---")
            if ticket.get("ai_is_recurring"):
                st.warning("🔄 Ticket ricorrente")
            if ticket.get("ai_root_cause"):
                st.markdown(f"**Causa radice:** {ticket['ai_root_cause']}")
            if pd.notna(ticket.get("ai_urgency_score")):
                urgency = int(ticket["ai_urgency_score"])
                color = "🔴" if urgency >= 4 else "🟡" if urgency == 3 else "🟢"
                st.markdown(f"**Urgenza:** {color} {urgency}/5")

            tags = ticket.get("ai_pattern_tags")
            tag_list: list[str] = []
            if isinstance(tags, list):
                tag_list = [str(t) for t in tags]
            elif isinstance(tags, str) and tags.strip():
                tag_list = [tags]
            if tag_list:
                st.markdown("**Tag:** " + " ".join(f"`{html.escape(t)}`" for t in tag_list))

            if ticket.get("ai_suggested_action"):
                st.markdown("---")
                st.markdown("**💡 Azione suggerita:**")
                st.info(str(ticket["ai_suggested_action"]))

            st.markdown("---")
            st.subheader("⚠️ Alert")
            if ticket.get("has_sensitive_data"):
                st.error(
                    "**Dati sensibili rilevati:** possibili password, credenziali, CF, IBAN, "
                    "numeri di carta o altre informazioni riservate nel contenuto del ticket."
                )
            al = str(ticket.get("alert_level") or "ok").lower()
            st.markdown(f"**Livello alert:** {alert_level_display(al)}")

            issue_blocks = [
                ("🔴 Sicurezza", "security_issues"),
                ("🟡 Qualità", "quality_issues"),
                ("🟠 Comunicazione", "communication_issues"),
                ("🔵 Tecnico", "technical_issues"),
                ("⚫ Processo", "process_issues"),
            ]
            any_issue = False
            for label, key in issue_blocks:
                arr = ticket.get(key)
                if issues_nonempty(arr):
                    any_issue = True
                    st.markdown(f"**{label}:** `{', '.join(str(x) for x in arr)}`")
            if not any_issue and not ticket.get("has_sensitive_data"):
                st.caption("Nessun problema aggiuntivo segnalato dall’analisi.")
        else:
            st.info("Ticket non ancora analizzato dall'AI.")
