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


def run_sync() -> int:
    """Esegue sync API → DB. Ritorna quante INSERT hanno inserito una nuova riga."""
    client = PulsewayClient()
    issue_types = client.get_issue_types() or []

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

                iid = it.get("id")
                if iid is None:
                    continue
                try:
                    iid_int = int(iid)
                except (TypeError, ValueError):
                    logger.warning(f"sync_categories: id issue type non valido: {iid}")
                    continue

                try:
                    sub_types = client.get_sub_issue_types(iid_int) or []
                except Exception as e:
                    logger.warning(
                        f"sync_categories: sottocategorie per issue_type_id={iid_int} ({pc}): {e}"
                    )
                    continue

                for sub in sub_types:
                    sn = _issue_name(sub)
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
