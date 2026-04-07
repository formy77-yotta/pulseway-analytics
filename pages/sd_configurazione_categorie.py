"""
sd_configurazione_categorie.py — Mappatura categorie Pulseway → categorie AI (Gemini).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from ai_categories_constants import CATEGORIE
from config import DATABASE_URL
from database import CREATE_CATEGORY_MAPPING, get_conn
import sync_categories

st.title("🗂️ Mappatura categorie Pulseway → AI")
st.caption(
    "Sincronizza le IssueType da Pulseway, poi assegna una categoria AI standard. "
    "La mappatura è usata da `analyze_tickets.py` per il campo `category_match`."
)


@st.cache_data(ttl=120)
def load_mapping() -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    try:
        df = pd.read_sql(
            """
            SELECT id, pulseway_category, pulseway_sub, ai_category, ai_subcategory,
                   is_equivalent, note, created_at, updated_at
            FROM category_mapping
            ORDER BY pulseway_category, pulseway_sub
            """,
            engine,
        )
    except Exception:
        df = pd.DataFrame()
    for col in ("created_at", "updated_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def ensure_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_CATEGORY_MAPPING)


def save_mapping(df: pd.DataFrame) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                rid = int(row["id"])
                ai_cat = row.get("ai_category")
                if pd.isna(ai_cat) or (isinstance(ai_cat, str) and not str(ai_cat).strip()):
                    ai_cat = None
                else:
                    ai_cat = str(ai_cat).strip()
                ai_sub = row.get("ai_subcategory")
                if pd.isna(ai_sub) or (isinstance(ai_sub, str) and not str(ai_sub).strip()):
                    ai_sub = None
                else:
                    ai_sub = str(ai_sub).strip()
                note = row.get("note")
                if pd.isna(note):
                    note = None
                else:
                    note = str(note)
                cur.execute(
                    """
                    UPDATE category_mapping
                    SET ai_category = %s,
                        ai_subcategory = %s,
                        is_equivalent = %s,
                        note = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (ai_cat, ai_sub, bool(row["is_equivalent"]), note, rid),
                )


try:
    ensure_table()
except Exception as e:
    st.error(f"❌ Errore DB: {e}")
    st.stop()

df = load_mapping()

if st.button("🔄 Sincronizza da Pulseway", type="secondary"):
    try:
        n = sync_categories.run_sync()
        st.cache_data.clear()
        st.success(f"Sincronizzazione completata. Nuove righe inserite: {n}")
        st.rerun()
    except Exception as e:
        st.error(f"Errore sync: {e}")

if df.empty:
    st.info(
        "Nessuna categoria in tabella. Clicca **Sincronizza da Pulseway** per importare "
        "IssueType e SubIssueType."
    )
    st.stop()

opts = [""] + list(CATEGORIE)

edited = st.data_editor(
    df,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    disabled=["id", "pulseway_category", "pulseway_sub", "created_at", "updated_at"],
    column_config={
        "pulseway_category": st.column_config.TextColumn("Categoria Pulseway", width="medium"),
        "pulseway_sub": st.column_config.TextColumn("Sottocategoria Pulseway", width="medium"),
        "ai_category": st.column_config.SelectboxColumn(
            "Categoria AI",
            options=opts,
            width="medium",
        ),
        "ai_subcategory": st.column_config.TextColumn("Sottocategoria AI (suggerita)", width="medium"),
        "is_equivalent": st.column_config.CheckboxColumn("Equivalente (match)", width="small"),
        "note": st.column_config.TextColumn("Note", width="large"),
        "created_at": st.column_config.DatetimeColumn("Creato", disabled=True),
        "updated_at": st.column_config.DatetimeColumn("Aggiornato", disabled=True),
    },
)

if st.button("💾 Salva mappatura", type="primary"):
    try:
        save_mapping(edited)
        st.cache_data.clear()
        st.success("✅ Mappatura salvata.")
        st.rerun()
    except Exception as e:
        st.error(f"❌ Salvataggio fallito: {e}")
