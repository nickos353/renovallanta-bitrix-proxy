# main.py
import os
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import aiohttp
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

# ----------------------------
# Configuración
# ----------------------------
BITRIX_BASE = (os.getenv("BITRIX_WEBHOOK_BASE") or "").rstrip("/")
API_KEY = os.getenv("API_KEY")  # si no está definido, no se exige clave

if not BITRIX_BASE:
    raise RuntimeError("Falta la variable de entorno BITRIX_WEBHOOK_BASE")


# ----------------------------
# App
# ----------------------------
app = FastAPI(title="Renovallanta Bitrix Proxy", version="1.3.2")


# ----------------------------
# Utilidades Bitrix
# ----------------------------
async def _bitrix_call(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Llama a un método REST de Bitrix.
    URL final: {BITRIX_BASE}/{method}.json
    Envía los params como JSON. Maneja errores de Bitrix.
    """
    url = f"{BITRIX_BASE}/{method}.json"
    async with aiohttp.ClientSession(raise_for_status=False) as session:
        async with session.post(url, json=params) as resp:
            # Aceptamos 200/400/401 etc., y devolvemos el JSON para inspección
            try:
                data = await resp.json()
            except Exception:
                text = await resp.text()
                raise HTTPException(
                    status_code=502,
                    detail=f"Bitrix sin JSON válido. HTTP {resp.status}: {text[:300]}",
                )
            # Bitrix puede responder 200 con {"error": "..."}
            if "error" in data:
                raise HTTPException(status_code=400, detail=data)
            return data


async def bitrix_fetch_all(method: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Llama repetidamente al endpoint de Bitrix usando paginación 'start'
    hasta obtener todos los elementos.
    """
    items: List[Dict[str, Any]] = []
    start: Optional[int] = 0

    while True:
        page_params = dict(params)
        if start is not None:
            page_params["start"] = start
        data = await _bitrix_call(method, page_params)

        # La forma del resultado varía por método
        result = data.get("result")

        if isinstance(result, dict):
            # Ej.: tasks.task.list devuelve un dict con 'tasks' o con las tareas directas
            if "tasks" in result and isinstance(result["tasks"], list):
                items.extend(result["tasks"])
            else:
                # Si fuera un dict de eventos u otro, intenta aplanar listas internas
                for v in result.values():
                    if isinstance(v, list):
                        items.extend(v)
        elif isinstance(result, list):
            items.extend(result)

        next_start = data.get("next")
        if next_start is None:
            break
        start = next_start

    return items


def _fmt(dt: datetime) -> str:
    """Formatea datetimes a 'YYYY-MM-DDTHH:MM:SS' (sin zona horaria)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# ----------------------------
# Modelos de entrada
# ----------------------------
class ActivitiesIn(BaseModel):
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    responsible_ids: Optional[List[int]] = None
    completed: Optional[str] = None  # "Y" | "N"
    types: Optional[List[str]] = None  # CALL, MEETING, EMAIL, TASK


class TasksIn(BaseModel):
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    responsible_ids: Optional[List[int]] = None
    member_ids: Optional[List[int]] = None
    group_ids: Optional[List[int]] = None
    status: Optional[int] = None
    # Campo de fecha a usar en el filtro principal:
    # CREATED_DATE | CLOSED_DATE | DEADLINE
    date_field: Optional[str] = "CREATED_DATE"


class CalendarIn(BaseModel):
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    owner_ids: Optional[List[int]] = None


# ----------------------------
# Autenticación
# ----------------------------
async def ensure_api_key(x_api_key: Optional[str] = Header(default=None)):
    """
    Exige API Key si API_KEY está configurada.
    """
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida")


# ----------------------------
# Endpoints básicos
# ----------------------------
@app.get("/")
async def root():
    return {"ok": True, "service": "renovallanta-bitrix-proxy"}


# Diagnóstico: comprueba si llega la API Key (por header o query)
async def _get_any_key(
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
) -> Optional[str]:
    return x_api_key or api_key


@app.get("/auth_check")
async def auth_check(
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    got = await _get_any_key(x_api_key, api_key)
    valid = (API_KEY is None) or (got == API_KEY)
    return {
        "ok": bool(valid),
        "got_header": x_api_key is not None,
        "got_query": api_key is not None,
    }


# ----------------------------
# Users
# ----------------------------
@app.get("/users")
async def list_users(x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    # Puedes usar 'user.search' o 'user.get'. Aquí usamos 'user.get' y filtramos activos.
    params: Dict[str, Any] = {
        # sin filtros; Bitrix no siempre acepta 'ACTIVE' en user.get,
        # así que filtramos luego en Python.
    }
    rows = await bitrix_fetch_all("user.get", params)

    # Quedarnos solo con los activos (ACTIVE == "Y")
    active = [u for u in rows if str(u.get("ACTIVE", "Y")).upper() == "Y"]

    # Reducimos a lo esencial si quieres devolver más limpio:
    items = [
        {
            "ID": u.get("ID"),
            "NAME": u.get("NAME"),
            "LAST_NAME": u.get("LAST_NAME"),
            "WORK_POSITION": u.get("WORK_POSITION"),
        }
        for u in active
    ]
    return {"items": items, "count": len(items)}


# ----------------------------
# Tasks
# ----------------------------
@app.post("/tasks")
async def list_tasks(payload: TasksIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now

    # Qué campo de fecha usar
    date_field = (payload.date_field or "CREATED_DATE").upper()
    if date_field not in {"CREATED_DATE", "CLOSED_DATE", "DEADLINE"}:
        raise HTTPException(
            status_code=422,
            detail="date_field debe ser CREATED_DATE, CLOSED_DATE o DEADLINE",
        )

    filt: Dict[str, Any] = {
        f">={date_field}": _fmt(from_dt),
        f"<={date_field}": _fmt(to_dt),
    }

    if payload.responsible_ids:
        filt["RESPONSIBLE_ID"] = payload.responsible_ids
    if payload.member_ids:
        filt["MEMBER_ID"] = payload.member_ids
    if payload.group_ids:
        filt["GROUP_ID"] = payload.group_ids
    if payload.status is not None:
        filt["STATUS"] = payload.status

    params: Dict[str, Any] = {
        "filter": filt,
        "select": [
            "ID",
            "TITLE",
            "STATUS",
            "RESPONSIBLE_ID",
            "CREATED_DATE",
            "CLOSED_DATE",
            "DEADLINE",
            "UF_CRM_TASK",
            "GROUP_ID",
            "PRIORITY",
            "UF_MAIL_MESSAGE",
            "AUDITORS",
            "ACCOMPLICES",
            "CREATED_BY",
        ],
        "order": {"CREATED_DATE": "DESC"},
    }

    rows = await bitrix_fetch_all("tasks.task.list", params)
    return {"items": rows, "count": len(rows)}


# ----------------------------
# Calendar
# ----------------------------
@app.post("/calendar")
async def list_calendar(payload: CalendarIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now
    owner_ids = payload.owner_ids or []

    items: List[Dict[str, Any]] = []

    # La API de calendario suele requerir ownerId y type='user' por cada dueño
    # (no tiene paginación start de la misma manera).
    for owner_id in owner_ids:
        params = {
            "type": "user",
            "ownerId": owner_id,
            "from": _fmt(from_dt),
            "to": _fmt(to_dt),
            "attendees": "N",
        }
        data = await _bitrix_call("calendar.event.get", params)
        result = data.get("result", {})
        # La clave suele ser 'items'
        events = result.get("items", [])
        if isinstance(events, list):
            items.extend(events)

    return {"items": items, "count": len(items)}
