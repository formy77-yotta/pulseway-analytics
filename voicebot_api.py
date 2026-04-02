"""
voicebot_api.py — FastAPI endpoint per ElevenLabs Conversational AI.
Protetto da API Key tramite header X-API-Key.

Avvio locale:  uvicorn voicebot_api:app --reload --port 8000
Su Render:     viene avviato tramite render.yaml
"""

import os
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
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

# ------------------------------------------------------------------
# API Key auth
# ------------------------------------------------------------------
API_KEY = os.environ.get("VOICEBOT_API_KEY", "")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(key: str = Security(api_key_header)):
    if not API_KEY:
        return  # Se non configurata, skip (utile in dev)
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="API Key non valida o mancante")

# ------------------------------------------------------------------
# Client Pulseway — unica istanza, gestisce token automaticamente
# ------------------------------------------------------------------
client = PulsewayClient()


# ------------------------------------------------------------------
# Modelli input
# ------------------------------------------------------------------

class LookupContactRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None

class OpenTicketsRequest(BaseModel):
    account_id: int
    contact_id: Optional[int] = None
    max_results: Optional[int] = 5

class CreateTicketRequest(BaseModel):
    account_id: int
    title: str
    description: Optional[str] = None
    contact_id: Optional[int] = None
    location_id: Optional[int] = None
    priority_id: Optional[int] = 2
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


@app.post("/tools/lookup_contact", dependencies=[Depends(verify_api_key)])
def lookup_contact(req: LookupContactRequest):
    if not req.name and not req.email:
        raise HTTPException(status_code=400, detail="Fornire almeno name o email.")

    logger.info(f"lookup_contact: name={req.name} email={req.email}")
    contact = client.lookup_contact(name=req.name, email=req.email)
    logger.info(f"lookup_contact result: {contact}")

    if not contact:
        return {
            "found": False,
            "message": "Nessun contatto trovato.",
        }

    return {"found": True, **contact}


@app.post("/tools/get_open_tickets", dependencies=[Depends(verify_api_key)])
def get_open_tickets(req: OpenTicketsRequest):
    logger.info(f"get_open_tickets: account_id={req.account_id}")

    tickets = client.get_open_tickets_by_account(
        account_id=req.account_id,
        contact_id=req.contact_id,
        max_results=req.max_results,
    )

    if not tickets:
        return {"count": 0, "tickets": [], "message": "Nessun ticket aperto."}

    return {"count": len(tickets), "tickets": tickets}


@app.post("/tools/create_ticket", dependencies=[Depends(verify_api_key)])
def create_ticket(req: CreateTicketRequest):
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


@app.post("/tools/get_account_info", dependencies=[Depends(verify_api_key)])
def get_account_info(req: AccountInfoRequest):
    logger.info(f"get_account_info: account_id={req.account_id}")

    info = client.get_account_info(req.account_id)
    if not info:
        raise HTTPException(status_code=404, detail="Account non trovato.")

    return info
