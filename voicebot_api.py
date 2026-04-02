"""
voicebot_api.py — FastAPI endpoint per ElevenLabs Conversational AI.
Deployare su Render come servizio separato.

Avvio locale:  uvicorn voicebot_api:app --reload --port 8000
Su Render:     viene avviato tramite render.yaml
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from loguru import logger
from api_client import PulsewayClient

app = FastAPI(title="Pulseway Voicebot API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Client Pulseway — unica istanza, gestisce token automaticamente
client = PulsewayClient()


# ------------------------------------------------------------------
# Modelli input
# ------------------------------------------------------------------

class LookupContactRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None

class OpenTicketsRequest(BaseModel):
    account_id: int
    max_results: Optional[int] = 5

class CreateTicketRequest(BaseModel):
    account_id: int
    title: str
    description: Optional[str] = None
    contact_id: Optional[int] = None
    location_id: Optional[int] = None
    priority_id: Optional[int] = 2   # 1=Low 2=Medium 3=High 4=Critical
    type_id: Optional[int] = None
    queue_id: Optional[int] = None

class AccountInfoRequest(BaseModel):
    account_id: int


# ------------------------------------------------------------------
# Endpoint
# ------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/tools/lookup_contact")
def lookup_contact(req: LookupContactRequest):
    """
    Cerca il cliente su Pulseway per nome o email.
    ElevenLabs chiama questo appena conosce il nome del cliente.
    """
    if not req.name and not req.email:
        raise HTTPException(status_code=400, detail="Fornire almeno name o email.")

    logger.info(f"lookup_contact: name={req.name} email={req.email}")

    contact = client.lookup_contact(name=req.name, email=req.email)

    if not contact:
        return {
            "found": False,
            "message": "Nessun contatto trovato. Procedo raccogliendo i dati manualmente.",
        }

    return {"found": True, **contact}


@app.post("/tools/get_open_tickets")
def get_open_tickets(req: OpenTicketsRequest):
    """
    Recupera i ticket aperti dell'azienda del cliente.
    ElevenLabs chiama questo se il cliente chiede lo stato delle sue richieste.
    """
    logger.info(f"get_open_tickets: account_id={req.account_id}")

    tickets = client.get_open_tickets_by_account(
        account_id=req.account_id,
        max_results=req.max_results,
    )

    if not tickets:
        return {"count": 0, "tickets": [], "message": "Nessun ticket aperto."}

    return {"count": len(tickets), "tickets": tickets}


@app.post("/tools/create_ticket")
def create_ticket(req: CreateTicketRequest):
    """
    Crea un nuovo ticket su Pulseway con le info raccolte dal voicebot.
    ElevenLabs chiama questo alla fine della conversazione.
    """
    logger.info(f"create_ticket: account_id={req.account_id} title={req.title}")

    try:
        result = client.create_ticket(
            account_id=req.account_id,
            title=req.title,
            description=req.description,
            contact_id=req.contact_id,
            location_id=req.location_id,
            priority_id=req.priority_id or 2,
            type_id=req.type_id,
            queue_id=req.queue_id,
        )
        return {
            "success": True,
            "message": f"Ticket #{result['ticketNumber']} creato con successo.",
            **result,
        }
    except RuntimeError as e:
        raise HTTPException(status_code=422, detail=str(e))


@app.post("/tools/get_account_info")
def get_account_info(req: AccountInfoRequest):
    """Info aggiuntive sull'azienda del cliente."""
    logger.info(f"get_account_info: account_id={req.account_id}")

    info = client.get_account_info(req.account_id)
    if not info:
        raise HTTPException(status_code=404, detail="Account non trovato.")

    return info
