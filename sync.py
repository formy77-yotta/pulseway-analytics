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
from database import init_db, upsert_tickets, get_ticket_count


def sync(days: int = None):
    init_db()
    client = PulsewayClient()

    filters = {}
    if days:
        from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
        filters["Filter.OpenDateFrom"] = from_date
        logger.info(f"Sync incrementale: ultimi {days} giorni")
    else:
        logger.info("Sync completo di tutti i ticket...")

    tickets = client.get_all_tickets(extra_filters=filters or None)

    if not tickets:
        logger.warning("Nessun ticket ricevuto.")
        return

    upsert_tickets(tickets)
    total = get_ticket_count()
    logger.success(f"✅ Sync OK! Questa sessione: {len(tickets)} | Totale DB: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=None,
                        help="Scarica solo gli ultimi N giorni (default: tutti)")
    args = parser.parse_args()
    sync(days=args.days)
