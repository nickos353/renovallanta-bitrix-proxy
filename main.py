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

# Nuevo: offset de zona horaria para Bitrix (p. ej. "-05:00", "+03:00" o "Z")
TZ_OFFSET = (os.getenv("BITRIX_TZ_OFFSET") or "Z").strip()

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

def _fmt_with_offset(dt: datetime) -> str:
    """
    Devuelve 'YYYY-MM-DDTHH:MM:SS' + offset.
    Si TZ_OFFSET = 'Z' -> agrega 'Z'.
    Si TZ_OFFSET = '-05:00' -> agrega '-05:00'.
    """
    base = _fmt(dt)
    off = TZ_OFFSET
    if off.upper() == "Z":
        return base + "Z"
    # Validación súper simple
    if not (off.startswith("+") or off.startswith("-")) or len(off) not in (6, 3):
        # Por seguridad, si el formato es raro, usa 'Z'
        return base + "Z"
    return base + off

def log(*args: Any) -> None:
    print(*args, file=sys.stdout, flush=True)


# ----------------------------
# App
# ----------------------------
app = FastAPI(title="Renovallanta Bitrix Proxy", version="1.5.1")

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
    return {"ok": True, "service": "Bitrix Proxy API is online"}


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
# Users
# ----------------------------
def _truthy(v):
    # Acepta True/False, "Y"/"N", "1"/"0", "true"/"false", etc.
    if isinstance(v, bool):
        return v
    if v is None:
        return True
    return str(v).strip().upper() in {"Y", "YES", "SI", "TRUE", "1"}

@app.get("/users")
async def list_users(x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    # 1) Intento principal
    rows = await bitrix_fetch_all("user.get", {})

    # 2) Fallback por si el portal restringe user.get
    if not rows:
        rows = await bitrix_fetch_all("user.search", {"ACTIVE": "true"})

    # Filtra activos con tolerancia a formato
    active = [u for u in rows if _truthy(u.get("ACTIVE", True))]

    items = [
        {
            "ID": u.get("ID"),
            "NAME": u.get("NAME"),
            "LAST_NAME": u.get("LAST_NAME"),
            "WORK_POSITION": u.get("WORK_POSITION"),
            "ACTIVE": u.get("ACTIVE"),
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
# Calendar (con fallback de zona horaria)
# ----------------------------
@app.post("/calendar")
async def list_calendar(payload: CalendarIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now
    owner_ids = payload.owner_ids or []

    if not owner_ids:
        return {"items": [], "count": 0}

    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    async def _fetch_for_owner(owner_id: int) -> None:
        # 1) intento sin offset
        base_params = {
            "type": "user",
            "ownerId": owner_id,
            "from": _fmt(from_dt),
            "to": _fmt(to_dt),
            "attendees": "N",
        }
        try:
            data = await _bitrix_call("calendar.event.get", base_params)
            result = data.get("result", {})
            evs = result.get("items", [])
            if isinstance(evs, list):
                items.extend(evs)
            return
        except HTTPException as e1:
            errors.append(f"owner {owner_id} naive error: {e1.detail}")

        # 2) fallback con offset/Z
        tz_params = {
            "type": "user",
            "ownerId": owner_id,
            "from": _fmt_with_offset(from_dt),
            "to": _fmt_with_offset(to_dt),
            "attendees": "N",
        }
        try:
            data = await _bitrix_call("calendar.event.get", tz_params)
            result = data.get("result", {})
            evs = result.get("items", [])
            if isinstance(evs, list):
                items.extend(evs)
            return
        except HTTPException as e2:
            errors.append(f"owner {owner_id} tz error: {e2.detail}")

    # Ejecuta en serie para evitar rate-limit
    for oid in owner_ids:
        await _fetch_for_owner(int(oid))

    if not items and errors:
        raise HTTPException(status_code=502, detail={"message": "calendar fetch failed", "errors": errors})

    return {"items": items, "count": len(items)}

# ----------------------------
# Task comments (opcional pero recomendado)
# ----------------------------
class TaskCommentsIn(BaseModel):
    task_ids: List[int]
    limit_per_task: Optional[int] = 5  # 0 = sin límite

def _pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

@app.post("/task_comments")
async def task_comments(payload: TaskCommentsIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    out: Dict[str, List[Dict[str, Any]]] = {}

    # Posibles métodos REST para comentarios según versión/portal
    candidates = [
        "task.commentitem.getlist",   # legacy
        "task.commentitem.getList",   # legacy camel
        "tasks.task.getComments",     # nuevo en algunos portales
    ]

    for tid in payload.task_ids:
        comments: List[Dict[str, Any]] = []

        for method in candidates:
            try:
                # Cada método puede esperar nombres distintos
                params = {"taskId": int(tid)}
                data = await _bitrix_call(method, params)
                res = data.get("result", [])

                # Normalizar distintas formas de respuesta
                if isinstance(res, dict):
                    res = res.get("comments") or res.get("items") or []

                # Mapear a un formato plano y amigable
                for c in (res or []):
                    comments.append({
                        "id": _pick(c, "ID", "id"),
                        "author_id": _pick(c, "AUTHOR_ID", "authorId", "AUTHOR"),
                        "author_name": (_pick(c, "AUTHOR_NAME", "authorName")
                                        or (_pick(c, "AUTHOR", "author") or {}).get("name")),
                        "created_at": _pick(c, "POST_DATE", "createdDate", "dateCreate", "DATE_CREATE"),
                        "text": (_pick(c, "POST_MESSAGE", "text", "message") or "").strip(),
                    })
                if comments:
                    break  # ya obtuvimos comentarios con este método
            except HTTPException:
                # Bitrix puede devolver {"error": "..."} si el método no existe en el plan/portal
                continue
            except Exception:
                continue

        # Ordenar por fecha y recortar
        comments = sorted(comments, key=lambda x: x.get("created_at") or "")
        if (payload.limit_per_task or 0) > 0:
            comments = comments[-int(payload.limit_per_task):]

        out[str(tid)] = comments

    return {"items": out, "count": sum(len(v) for v in out.values())}


# ----------------------------
# Run local
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
