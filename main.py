#!/usr/bin/env python3
"""
WhatsApp Lead Tracker — via Chatwoot + Baileys
Recebe webhooks do Chatwoot, extrai ctwa_clid (origem do anúncio)
e registra o contato em uma planilha do Google Sheets.
"""

import os
from dotenv import load_dotenv
load_dotenv()
import requests
import gspread
from datetime import datetime
from fastapi import FastAPI, Request, Header, HTTPException
from google.oauth2.service_account import Credentials

# ── CONFIGURAÇÕES ─────────────────────────────────────────────────────────────

META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")
CHATWOOT_TOKEN    = os.environ.get("CHATWOOT_WEBHOOK_TOKEN", "")  # opcional, para segurança
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID", "")
SHEET_NAME        = os.environ.get("SHEET_NAME", "Leads")
SERVICE_ACCOUNT   = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "service_account.json")

BASE_URL = "https://graph.facebook.com/v19.0"

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────

def get_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        return spreadsheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=10)


def garantir_cabecalho(sheet):
    if not sheet.row_values(1):
        sheet.append_row([
            "Data", "Telefone", "Nome", "Mensagem",
            "Campanha", "Conjunto", "Anúncio",
            "ctwa_clid", "Conversa Chatwoot"
        ])


def registrar_lead(dados: dict):
    sheet = get_sheet()
    garantir_cabecalho(sheet)
    sheet.append_row([
        dados.get("data", ""),
        dados.get("phone", ""),
        dados.get("name", ""),
        dados.get("message", ""),
        dados.get("campaign_name", ""),
        dados.get("adset_name", ""),
        dados.get("ad_name", ""),
        dados.get("ctwa_clid", ""),
        dados.get("conversa_id", ""),
    ])
    print(f"[{dados['data']}] Lead registrado: {dados.get('name') or dados.get('phone')} | campanha: {dados.get('campaign_name', '—')}")


# ── META ADS ──────────────────────────────────────────────────────────────────

def buscar_dados_anuncio(ad_id: str) -> dict:
    try:
        r = requests.get(
            f"{BASE_URL}/{ad_id}",
            params={
                "fields": "id,name,adset{id,name},campaign{id,name}",
                "access_token": META_ACCESS_TOKEN,
            },
            timeout=10,
        )
        d = r.json()
        if "error" in d:
            return {}
        return {
            "ad_name":       d.get("name"),
            "adset_name":    d.get("adset", {}).get("name"),
            "campaign_name": d.get("campaign", {}).get("name"),
        }
    except Exception:
        return {}


# ── EXTRAIR ctwa_clid DO PAYLOAD DO CHATWOOT ──────────────────────────────────

def extrair_ctwa(payload: dict) -> dict:
    """
    O Chatwoot pode entregar o ctwa_clid em locais diferentes
    dependendo da versão e configuração do Baileys.
    Tentamos todos os caminhos conhecidos.
    """
    ctwa = None
    ad_id = None

    # Caminho 1: content_attributes da mensagem (mais comum no Chatwoot + Baileys)
    content_attrs = payload.get("content_attributes", {})
    ctwa  = ctwa  or content_attrs.get("ctwa_clid")
    ad_id = ad_id or content_attrs.get("ads_id")

    # Caminho 2: dentro de items[0] (algumas versões)
    for item in content_attrs.get("items", []):
        ctwa  = ctwa  or item.get("ctwa_clid")
        ad_id = ad_id or item.get("ads_id")

    # Caminho 3: additional_attributes da conversa
    conv = payload.get("conversation", {})
    add_attrs = conv.get("additional_attributes", {})
    ctwa  = ctwa  or add_attrs.get("ctwa_clid")
    ad_id = ad_id or add_attrs.get("ads_id")

    # Caminho 4: metadata da conversa (algumas integrações Baileys customizadas)
    meta = conv.get("meta", {})
    ctwa  = ctwa  or meta.get("ctwa_clid")
    ad_id = ad_id or meta.get("ads_id")

    return {"ctwa_clid": ctwa, "ad_id": ad_id}


# ── WEBHOOK ───────────────────────────────────────────────────────────────────

app = FastAPI()

# Conversas aguardando ctwa_clid (race condition: proxy injeta após conversation_created)
# { conversa_id: { phone, name, message, data } }
_conversas_pendentes: dict = {}


def _extrair_dados_contato(payload: dict) -> dict:
    contact = payload.get("meta", {}).get("sender", {})
    messages = payload.get("messages", [])
    return {
        "data":        datetime.now().strftime("%d/%m/%Y %H:%M"),
        "phone":       contact.get("phone_number", "").replace("+", "").replace(" ", ""),
        "name":        contact.get("name", ""),
        "message":     messages[0].get("content", "") if messages else "",
        "conversa_id": payload.get("id", ""),
    }


def _processar_lead(payload: dict, ctwa_info: dict):
    dados = _extrair_dados_contato(payload)
    dados["ctwa_clid"] = ctwa_info.get("ctwa_clid")

    ad_id = ctwa_info.get("ad_id")
    if ad_id and META_ACCESS_TOKEN:
        dados.update(buscar_dados_anuncio(ad_id))

    registrar_lead(dados)


@app.post("/webhook/chatwoot")
async def receber_chatwoot(
    request: Request,
    x_chatwoot_token: str = Header(None),
):
    if CHATWOOT_TOKEN and x_chatwoot_token != CHATWOOT_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")

    payload = await request.json()
    event = payload.get("event")
    conversa_id = payload.get("id")

    if event == "conversation_created":
        ctwa_info = extrair_ctwa(payload)

        if ctwa_info.get("ctwa_clid"):
            # ctwa_clid já presente — registra imediatamente
            _processar_lead(payload, ctwa_info)
            return {"status": "ok"}

        # Sem ctwa_clid ainda — guarda como pendente (proxy pode injetar em seguida)
        _conversas_pendentes[conversa_id] = _extrair_dados_contato(payload)
        print(f"[PENDENTE] conversa {conversa_id} aguardando ctwa_clid")
        return {"status": "pending", "conversa_id": conversa_id}

    if event == "conversation_updated" and conversa_id in _conversas_pendentes:
        ctwa_info = extrair_ctwa(payload)

        if ctwa_info.get("ctwa_clid"):
            # ctwa_clid injetado pelo proxy — registra agora
            dados = _conversas_pendentes.pop(conversa_id)
            dados["ctwa_clid"] = ctwa_info.get("ctwa_clid")
            ad_id = ctwa_info.get("ad_id")
            if ad_id and META_ACCESS_TOKEN:
                dados.update(buscar_dados_anuncio(ad_id))
            registrar_lead(dados)
            return {"status": "ok", "via": "conversation_updated"}

    return {"status": "ignored", "event": event}


@app.get("/")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
