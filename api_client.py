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

    def _post(self, path: str, body: dict = None):
        url = f"{self.base_url}{path}"
        resp = requests.post(url, headers=self._headers(), json=body, timeout=30)
        if resp.status_code == 401:
            logger.warning("Token scaduto, rinnovo...")
            self._authenticate()
            resp = requests.post(url, headers=self._headers(), json=body, timeout=30)
        if not resp.ok:
            logger.error(f"HTTP {resp.status_code} su {path}: {resp.text}")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # TICKETS — usato da sync.py
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # CONTATTI — usato dal voicebot
    # ------------------------------------------------------------------

    def lookup_contact(self, name: str = None, email: str = None) -> dict | None:
        """
        Cerca un contatto per nome o email.
        Prova più strategie: email, nome, nome invertito, solo cognome.
        """
        contacts = []

        # 1. Cerca per email (più precisa)
        if email:
            data = self._get("/v2/crm/contacts/search", params={
                "Filter.EmailAddress": email,
                "PageSize": 5,
            })
            contacts = data.get("result", []) or []
            logger.info(f"Ricerca per email: {len(contacts)} risultati")

        # 2. Cerca per nome (FirstName + LastName)
        if not contacts and name:
            parts = name.strip().split()
            if len(parts) >= 2:
                params = {
                    "Filter.FirstName": parts[0],
                    "Filter.LastName":  " ".join(parts[1:]),
                    "PageSize": 10,
                }
                data = self._get("/v2/crm/contacts/search", params=params)
                contacts = data.get("result", []) or []
                logger.info(f"Ricerca nome normale: {len(contacts)} risultati")

        # 3. Prova invertendo (es. "Carlos Poggi" → FirstName=Poggi, LastName=Carlos)
        if not contacts and name:
            parts = name.strip().split()
            if len(parts) >= 2:
                params = {
                    "Filter.FirstName": parts[-1],
                    "Filter.LastName":  " ".join(parts[:-1]),
                    "PageSize": 10,
                }
                data = self._get("/v2/crm/contacts/search", params=params)
                contacts = data.get("result", []) or []
                logger.info(f"Ricerca nome invertito: {len(contacts)} risultati")

        # 4. Cerca solo per ogni termine (potrebbe essere solo cognome o solo nome)
        if not contacts and name:
            parts = name.strip().split()
            for term in parts:
                params = {"Filter.LastName": term, "PageSize": 10}
                data = self._get("/v2/crm/contacts/search", params=params)
                contacts = data.get("result", []) or []
                logger.info(f"Ricerca solo cognome '{term}': {len(contacts)} risultati")
                if contacts:
                    break

        if not contacts:
            logger.info(f"Nessun contatto trovato per: name={name} email={email}")
            return None

        c = contacts[0]
        logger.info(f"Contatto trovato: {c.get('firstName')} {c.get('lastName')} - {c.get('accountName')}")
        return {
            "contactId":    c.get("id"),
            "accountId":    c.get("accountId"),
            "accountName":  c.get("accountName"),
            "firstName":    c.get("firstName"),
            "lastName":     c.get("lastName"),
            "fullName":     f"{c.get('firstName', '')} {c.get('lastName', '')}".strip(),
            "email":        c.get("emailAddress"),
            "jobTitle":     c.get("jobTitle"),
            "locationId":   c.get("locationId"),
            "locationName": c.get("locationName"),
        }

    # ------------------------------------------------------------------
    # TICKET APERTI — usato dal voicebot
    # ------------------------------------------------------------------

    def get_open_tickets_by_account(self, account_id: int, max_results: int = 5, contact_id: int = None) -> list[dict]:
        """Restituisce i ticket aperti del contatto specifico, filtrando per contactId."""
        params = {
            "Filter.ContactId": contact_id,
            "Filter.ExcludeCompleted": 1,
            "PageSize": max_results,
            "PageNumber": 1,
        }
        data = self._get("/v2/servicedesk/tickets", params=params)
        tickets = data.get("result", []) or []
        logger.info(f"get_open_tickets: {len(tickets)} ticket trovati per contact {contact_id}")
        return [
            {
                "ticketId":     t.get("id"),
                "ticketNumber": t.get("ticketNumber"),
                "title":        t.get("title"),
                "status":       t.get("statusName"),
                "priority":     t.get("priorityName"),
                "openDate":     t.get("openDate"),
            }
            for t in tickets
        ]

    # ------------------------------------------------------------------
    # CREA TICKET — usato dal voicebot
    # ------------------------------------------------------------------

    def create_ticket(
        self,
        account_id: int,
        title: str,
        description: str = None,
        contact_id: int = None,
        location_id: int = None,
        priority_id: int = 2,
        type_id: int = None,
        queue_id: int = None,
    ) -> dict:
        """
        Crea un nuovo ticket su Pulseway.
        priority_id: 1=Low 2=Medium 3=High 4=Critical
        """
        from datetime import datetime, timezone

        # Mappa priorità 1-4 → ID reali Pulseway
        priority_map = {1: 40482, 2: 40483, 3: 40481, 4: 40484}
        real_priority = priority_map.get(int(priority_id or 2), 40483)

        payload = {
            "AccountId":  int(account_id),
            "Title":      str(title),
            "Details":    str(description or title),
            "PriorityId": real_priority,
            "TypeId":     8,       # Incident
            "StatusId":   49958,   # Nuovo
            "QueueId":    38402,   # YottaCore Support
            "OpenDate":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        }
        if contact_id:  payload["ContactId"]  = int(contact_id)
        if location_id: payload["LocationId"] = int(location_id)
        elif contact_id:
            # Recupera LocationId dal contatto se non passato esplicitamente
            try:
                contact_data = self._get(f"/v2/crm/contacts/summary/{contact_id}")
                loc_id = contact_data.get("result", {}).get("locationId")
                if loc_id:
                    payload["LocationId"] = int(loc_id)
                    logger.info(f"LocationId recuperato dal contatto: {loc_id}")
            except Exception as e:
                logger.warning(f"Impossibile recuperare LocationId: {e}")
        if type_id:     payload["TypeId"]     = int(type_id)
        if queue_id:    payload["QueueId"]    = int(queue_id)

        logger.info(f"create_ticket payload: {payload}")
        data = self._post("/v2/servicedesk/tickets", payload)
        logger.info(f"create_ticket response: {data}")

        if not data.get("success"):
            msg = data.get("error", {}).get("message", "Errore sconosciuto")
            raise RuntimeError(f"Pulseway create_ticket error: {msg}")

        result = data["result"]
        return {
            "ticketId":     result.get("id"),
            "ticketNumber": result.get("ticketNumber"),
            "title":        result.get("title"),
        }

    # ------------------------------------------------------------------
    # INFO ACCOUNT — usato dal voicebot
    # ------------------------------------------------------------------

    def get_account_info(self, account_id: int) -> dict | None:
        """Restituisce le info di riepilogo di un'azienda."""
        try:
            data = self._get(f"/v2/crm/accounts/{account_id}/summaryinfo")
            acc = data.get("result", {})
            return {
                "accountId": acc.get("id"),
                "name":      acc.get("name"),
                "phone":     acc.get("phone"),
                "email":     acc.get("email"),
                "status":    acc.get("statusName"),
                "type":      acc.get("typeName"),
            }
        except Exception as e:
            logger.warning(f"get_account_info({account_id}) fallito: {e}")
            return None
