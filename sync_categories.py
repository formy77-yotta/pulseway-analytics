#!/usr/bin/env python3
"""
Sincronizza IssueType / SubIssueType da Pulseway nella tabella category_mapping.
Le nuove categorie sono inserite con ai_category NULL (da mappare in Streamlit).
"""

from __future__ import annotations

from loguru import logger

from api_client import PulsewayClient
from database import CREATE_CATEGORY_MAPPING, get_conn


def _issue_name(obj: dict | None) -> str:
    if not obj:
        return ""
    if isinstance(obj, str):
        return obj.strip()
    n = (obj.get("name") or obj.get("Name") or obj.get("displayName") or "").strip()
    if n:
        return n
    oid = obj.get("id")
    return str(oid) if oid is not None else ""


def _parent_category_name(sub: dict, id_to_name: dict[int, str]) -> str:
    for key in (
        "issueTypeId",
        "issue_type_id",
        "parentIssueTypeId",
        "parentId",
        "IssueTypeId",
    ):
        pid = sub.get(key)
        if pid is not None:
            try:
                return id_to_name.get(int(pid), "") or ""
            except (TypeError, ValueError):
                pass
    pit = sub.get("issueType") or sub.get("parentIssueType")
    if isinstance(pit, dict):
        return _issue_name(pit)
    return ""


def run_sync() -> int:
    """Esegue sync API → DB. Ritorna quante INSERT hanno inserito una nuova riga."""
    client = PulsewayClient()
    issue_types = client.get_issue_types() or []
    sub_types = client.get_sub_issue_types() or []

    id_to_name: dict[int, str] = {}
    for it in issue_types:
        iid = it.get("id")
        if iid is not None:
            try:
                id_to_name[int(iid)] = _issue_name(it) or str(iid)
            except (TypeError, ValueError):
                pass

    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_CATEGORY_MAPPING)

            for it in issue_types:
                pc = _issue_name(it)
                if not pc:
                    continue
                cur.execute(
                    """
                    INSERT INTO category_mapping
                        (pulseway_category, pulseway_sub, ai_category, is_equivalent, note)
                    VALUES (%s, %s, NULL, TRUE, NULL)
                    ON CONFLICT (pulseway_category, pulseway_sub) DO NOTHING
                    """,
                    (pc, ""),
                )
                inserted += cur.rowcount

            for sub in sub_types:
                sn = _issue_name(sub)
                pc = _parent_category_name(sub, id_to_name)
                if not pc:
                    pc = _issue_name(sub.get("issueType") or sub.get("parent")) or "—"
                if not sn:
                    sn = str(sub.get("id", "—"))

                cur.execute(
                    """
                    INSERT INTO category_mapping
                        (pulseway_category, pulseway_sub, ai_category, is_equivalent, note)
                    VALUES (%s, %s, NULL, TRUE, NULL)
                    ON CONFLICT (pulseway_category, pulseway_sub) DO NOTHING
                    """,
                    (pc, sn),
                )
                inserted += cur.rowcount

    logger.success(f"sync_categories: nuove righe inserite: {inserted}")
    return inserted


if __name__ == "__main__":
    n = run_sync()
    print(f"Categorie nuove (insert effettivi): {n}")
