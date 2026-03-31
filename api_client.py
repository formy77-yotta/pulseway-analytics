"""
api_client.py — Client Pulseway PSA API v2.
Gestisce autenticazione Bearer e paginazione automatica.
"""

import requests
from loguru import logger
from config import SERVER_URL, USERNAME, PASSWORD, TENANT, PAGE_SIZE


class PulsewayClient:
    def __init__(self):
        self.base_url = f"https://{SERVER_URL}"
        self.token = None
        self._authenticate()

    def _authenticate(self):
        logger.info("Autenticazione Pulseway...")
        resp = requests.post(
            f"{self.base_url}/v2/security/authenticate",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grantType": "password",
                "userName": USERNAME,
                "password": PASSWORD,
                "tenant": TENANT,
            },
            timeout=15,
        )
        resp.raise_for_status()
        self.token = resp.json()["result"]["accessToken"]
        logger.success("Token ottenuto.")

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}

    def _get(self, path: str, params: dict = None):
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
        if resp.status_code == 401:
            logger.warning("Token scaduto, rinnovo...")
            self._authenticate()
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_all_tickets(self, extra_filters: dict = None) -> list[dict]:
        """Scarica tutti i ticket con paginazione automatica."""
        all_tickets = []
        page = 1

        while True:
            params = {"PageSize": PAGE_SIZE, "PageNumber": page}
            if extra_filters:
                params.update(extra_filters)

            logger.info(f"Pagina {page}...")
            data = self._get("/v2/servicedesk/tickets", params=params)
            items = data.get("result", [])

            if not items:
                break

            all_tickets.extend(items)
            logger.info(f"  → {len(items)} ticket (totale: {len(all_tickets)})")

            if len(items) < PAGE_SIZE:
                break

            page += 1

        logger.success(f"Download completato: {len(all_tickets)} ticket.")
        return all_tickets
