# main.py
import os
import sys
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import aiohttp
from fastapi import FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ----------------------------
# Configuración
# ----------------------------
_raw_base = os.getenv("BITRIX_WEBHOOK_BASE") or ""
BITRIX_BASE = _raw_base.strip()
API_KEY = os.getenv("API_KEY")  # si no está definido, no se exige clave

if not BITRIX_BASE:
    raise RuntimeError("Falta la variable de entorno BITRIX_WEBHOOK_BASE")

def _join_method(base: str, method: str) -> str:
    """Une base + método asegurando un único slash y sufijo .json."""
    b = base.rstrip("/")
    m = method.strip().lstrip("/")
    return f"{b}/{m}.json"

def _fmt(dt: datetime) -> str:
    """Formatea datetimes a 'YYYY-MM-DDTHH:MM:SS' (sin zona horaria)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def log(*args: Any) -> None:
    print(*args, file=sys.stdout, flush=True)


# ----------------------------
# App
# ----------------------------
app = FastAPI(title="Renovallanta Bitrix Proxy", version="1.4.2")

# CORS (ajústalo según tus necesidades)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Reutilizar una sola sesión HTTP
_session: Optional[aiohttp.ClientSession] = None

@app.on_event("startup")
async def _startup() -> None:
    global _session
    if _session is None:
        _session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    log("[startup] Proxy listo")

@app.on_event("shutdown")
async def _shutdown() -> None:
    global _session
    if _session is not None:
        await _session.close()
        _session = None
    log("[shutdown] Sesión HTTP cerrada")


# ----------------------------
# Utilidades Bitrix
# ----------------------------
async def _bitrix_call(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Llama a un método REST de Bitrix.
    URL final: {BITRIX_BASE}/{method}.json
    Envía los params como JSON. Maneja errores de Bitrix.
    """
    assert _session is not None, "HTTP session no inicializada"
    url = _join_method(BITRIX_BASE, method)
    async with _session.post(url, json=params) as resp:
        # Devolvemos el JSON (o error entendible) sin ocultar códigos HTTP
        try:
            data = await resp.json()
        except Exception:
            text = await resp.text()
            raise HTTPException(
                status_code=502,
                detail=f"Bitrix sin JSON válido. HTTP {resp.status}: {text[:300]}",
            )
        # Bitrix a veces responde 200 con {"error": "..."}
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

        result = data.get("result")

        if isinstance(result, dict):
            # Ej.: tasks.task.list devuelve un dict con 'tasks' o con listas internas
            if "tasks" in result and isinstance(result["tasks"], list):
                items.extend(result["tasks"])
            else:
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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key inválida")


# ----------------------------
# Endpoints de salud
# ----------------------------
@app.get("/")
async def root():
    return {"ok": True, "service": "renovallanta-bitrix-proxy"}

@app.get("/ping")
async def ping():
    return {"ok": True, "service": "renovallanta-bitrix-proxy"}


# ----------------------------
# Diagnóstico de auth
# ----------------------------
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
# Users (con búsqueda y paginación)
# ----------------------------
def _truthy(v):
    # Acepta True/False, "Y"/"N", "1"/"0", "true"/"false", etc.
    if isinstance(v, bool):
        return v
    if v is None:
        return True
    return str(v).strip().upper() in {"Y", "YES", "SI", "TRUE", "1"}

@app.get("/users")
async def list_users(
    x_api_key: Optional[str] = Header(default=None),
    q: Optional[str] = Query(None, description="Filtro por nombre/apellido"),
    include_inactive: bool = Query(False, description="Incluir usuarios inactivos"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    await ensure_api_key(x_api_key)

    # 1) Intento principal
    rows = await bitrix_fetch_all("user.get", {})

    # 2) Fallback por si el portal restringe user.get
    if not rows:
        rows = await bitrix_fetch_all("user.search", {"ACTIVE": "true"})

    # Filtrado por activos (salvo que pidan incluir inactivos)
    if not include_inactive:
        rows = [u for u in rows if _truthy(u.get("ACTIVE", True))]

    # Búsqueda simple por nombre completo
    if q:
        ql = q.lower()
        def full_name(u: Dict[str, Any]) -> str:
            return f"{u.get('NAME','')} {u.get('LAST_NAME','')}".strip()
        rows = [u for u in rows if ql in full_name(u).lower()]

    total = len(rows)
    # Paginación
    rows = rows[offset: offset + limit]

    items = [
        {
            "ID": u.get("ID"),
            "NAME": u.get("NAME"),
            "LAST_NAME": u.get("LAST_NAME"),
            "WORK_POSITION": u.get("WORK_POSITION"),
            "ACTIVE": u.get("ACTIVE"),
        }
        for u in rows
    ]
    return {"items": items, "count": len(items), "total": total, "offset": offset, "limit": limit}


# ----------------------------
# Tasks
# ----------------------------
@app.post("/tasks")
async def list_tasks(payload: TasksIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now

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
        events = result.get("items", [])
        if isinstance(events, list):
            items.extend(events)

    return {"items": items, "count": len(items)}


# ----------------------------
# Run local
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
