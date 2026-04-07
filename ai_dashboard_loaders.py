"""Caricamento dati condiviso tra pagine Streamlit AI (categorie, sentiment)."""

from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from config import DATABASE_URL
from queue_ticket_filter import (
    load_queue_config_dataframe,
    sql_ai_categories_join_filtered,
    sql_ai_categories_join_unfiltered,
)
from tickets_ai_schema import apply_tickets_ai_extra_migrations


@st.cache_data(ttl=300)
def load_queue_config_sidebar() -> pd.DataFrame:
    url = (DATABASE_URL or "").replace("postgres://", "postgresql://", 1)
    engine = create_engine(url)
    return load_queue_config_dataframe(engine)


@st.cache_data(ttl=300)
def load_ai_ticket_data() -> pd.DataFrame:
    url = (DATABASE_URL or "").replace("postgres://", "postgresql://", 1)
    engine = create_engine(url)
    apply_tickets_ai_extra_migrations(engine)
    try:
        df = pd.read_sql(sql_ai_categories_join_filtered(), engine)
    except Exception:
        df = pd.read_sql(sql_ai_categories_join_unfiltered(), engine)
    for col in ("open_date", "completed_date", "analyzed_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    df["mese"] = df["open_date"].dt.to_period("M").astype(str)
    df["chiuso"] = df["completed_date"].notna()
    return df
