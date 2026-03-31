"""
sync.py — Scarica ticket da Pulseway e li salva in PostgreSQL.

Uso locale:    python sync.py
Incrementale:  python sync.py --days 7
Su Railway:    viene eseguito dal cron job (railway.json)
"""

import argparse
from datetime import datetime, timedelta
from loguru import logger
from api_client import PulsewayClient
from database import init_db, upsert_tickets, get_ticket_count, delete_removed_tickets


def sync(days: int = None):
    init_db()
    client = PulsewayClient()

    filters = {}
    if days:
        from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
        filters["Filter.OpenDateFrom"] = from_date
        logger.info(f"Sync incrementale: ultimi {days} giorni")
    else:
        # Default: dal 01/01/2025
        filters["Filter.OpenDateFrom"] = "2025-01-01T00:00:00"
        logger.info("Sync completo dal 01/01/2025...")

    tickets = client.get_all_tickets(extra_filters=filters)

    if not tickets:
        logger.warning("Nessun ticket ricevuto.")
        return

    upsert_tickets(tickets)

    # Riconciliazione
    if not days:
        remote_ids = {t["id"] for t in tickets if t.get("id")}
        print(f"DEBUG - remote_ids esempio: {list(remote_ids)[:5]}")
        print(f"DEBUG - tipo ID remoto: {type(list(remote_ids)[0])}")
        deleted = delete_removed_tickets(remote_ids, from_date="2025-01-01T00:00:00")
        if deleted:
            logger.info(f"🗑️ Rimossi {deleted} ticket eliminati da Pulseway.")

    total = get_ticket_count()
    logger.success(f"✅ Sync OK! Questa sessione: {len(tickets)} | Totale DB: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=None)
    args = parser.parse_args()
    try:
        sync(days=args.days)
    except Exception as e:
        print(f"ERRORE: {e}")
        import traceback
        traceback.print_exc()