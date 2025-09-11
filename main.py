import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

# ===================== Config =====================
BITRIX_BASE_URL = (os.getenv("BITRIX_BASE_URL") or "").rstrip("/") + "/"
API_KEY = os.getenv("API_KEY")

app = FastAPI(title="Renovallanta Bitrix Aggregator", version="1.3.0")


# ===================== Utilidades =====================
async def bitrix_call(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not BITRIX_BASE_URL or "bitrix24" not in BITRIX_BASE_URL:
        raise RuntimeError("BITRIX_BASE_URL no configurado")
    url = BITRIX_BASE_URL + method
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, json=params)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise HTTPException(status_code=400, detail=data)
        return data


async def bitrix_fetch_all(method: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    start = 0
    while True:
        payload = dict(params)
        payload["start"] = start
        data = await bitrix_call(method, payload)
        result = data.get("result") or {}
        if isinstance(result, list):
            chunk = result
        elif isinstance(result, dict) and "tasks" in result:
            chunk = result.get("tasks") or []
        elif isinstance(result, dict) and "items" in result:
            chunk = result.get("items") or []
        else:
            chunk = result.get("activities") or result.get("events") or []
        items.extend(chunk)
        next_start = data.get("next")
        if next_start is None:
            break
        start = next_start
    return items


# ===================== Modelos =====================
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
    member_ids: Optional[List[int]] = None     # responsable/creador/participante/observador
    group_ids: Optional[List[int]] = None      # IDs de grupos/proyectos (Visitas)
    status: Optional[List[int]] = None
    date_field: Optional[str] = "DEADLINE"     # DEADLINE | CREATED_DATE | CLOSED_DATE


class CalendarIn(BaseModel):
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    owner_ids: Optional[List[int]] = None


# ===================== Auth =====================
async def ensure_api_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida")


# ===================== Endpoints =====================
@app.get("/")
async def root():
    return {"ok": True, "service": "renovallanta-bitrix-proxy"}


@app.get("/users")
async def list_users(x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)
    params = {"filter": {"ACTIVE": True},
              "select": ["ID", "NAME", "LAST_NAME", "WORK_POSITION", "DEPARTMENT"]}
    rows = await bitrix_fetch_all("user.get", params)
    return {"items": rows, "count": len(rows)}


@app.post("/activities")
async def list_activities(payload: ActivitiesIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)
    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now

    filt: Dict[str, Any] = {
        ">=CREATED": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "<=CREATED": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    if payload.responsible_ids:
        filt["RESPONSIBLE_ID"] = payload.responsible_ids
    if payload.completed in ("Y", "N"):
        filt["COMPLETED"] = payload.completed
    if payload.types:
        filt["TYPE_ID"] = payload.types

    params = {
        "filter": filt,
        "select": [
            "ID", "TYPE_ID", "SUBJECT", "CREATED", "RESPONSIBLE_ID", "COMPLETED",
            "DESCRIPTION", "BINDINGS", "DEADLINE", "AUTHOR_ID", "END_TIME"
        ],
        "order": {"CREATED": "DESC"}
    }
    rows = await bitrix_fetch_all("crm.activity.list", params)
    return {"items": rows, "count": len(rows)}


# ===================== /tasks con DEADLINE robusto =====================
def _dtstr(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _parse_deadline(v: Any) -> Optional[datetime]:
    if not v:
        return None
    s = str(v).strip()
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s + "T00:00:00")
        if " " in s and "T" not in s:
            s = s.replace(" ", "T")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


def _to_utc_aware(dt: datetime) -> datetime:
    """Devuelve dt como datetime *con tz* UTC para comparar de forma segura."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@app.post("/tasks")
async def list_tasks(payload: TasksIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now

    # Normalizamos rango a UTC-aware
    from_utc = _to_utc_aware(from_dt)
    to_utc = _to_utc_aware(to_dt)

    date_key = (payload.date_field or "DEADLINE").upper()
    if date_key not in {"DEADLINE", "CREATED_DATE", "CLOSED_DATE"}:
        date_key = "DEADLINE"

    # Filtro hacia Bitrix
    base_filter: Dict[str, Any] = {}
    if date_key == "DEADLINE":
        # Rango amplio por CREATED_DATE y luego filtramos local por DEADLINE
        wide_from = from_dt - timedelta(days=21)
        wide_to = to_dt + timedelta(days=7)
        base_filter[">=CREATED_DATE"] = _dtstr(wide_from)
        base_filter["<=CREATED_DATE"] = _dtstr(wide_to)
    else:
        base_filter[f">={date_key}"] = _dtstr(from_dt)
        base_filter[f"<={date_key}"] = _dtstr(to_dt)

    if payload.status:
        base_filter["STATUS"] = payload.status
    if payload.group_ids:
        base_filter["GROUP_ID"] = payload.group_ids

    selects = [
        "ID", "TITLE", "STATUS", "REAL_STATUS",
        "RESPONSIBLE_ID", "CREATED_BY",
        "CREATED_DATE", "CLOSED_DATE", "DEADLINE",
        "UF_CRM_TASK", "GROUP_ID", "PRIORITY",
        "AUDITORS", "ACCOMPLICES"
    ]

    async def fetch_with(extra: Dict[str, Any]) -> List[Dict[str, Any]]:
        params = {"filter": {**base_filter, **extra}, "select": selects, "order": {"ID": "DESC"}}
        return await bitrix_fetch_all("tasks.task.list", params)

    rows: List[Dict[str, Any]] = []
    # Si llegan ambos filtros, unimos resultados (algunas instalaciones no aceptan MEMBER+GROUP_ID)
    if payload.member_ids and payload.group_ids:
        seen: set[str] = set()
        for mid in payload.member_ids:
            for t in await fetch_with({"MEMBER": mid}):
                tid = str(t.get("ID"))
                if tid not in seen:
                    rows.append(t); seen.add(tid)
        for gid in payload.group_ids:
            for t in await fetch_with({"GROUP_ID": [gid]}):
                tid = str(t.get("ID"))
                if tid not in seen:
                    rows.append(t); seen.add(tid)
    elif payload.member_ids:
        seen: set[str] = set()
        for mid in payload.member_ids:
            for t in await fetch_with({"MEMBER": mid}):
                tid = str(t.get("ID"))
                if tid not in seen:
                    rows.append(t); seen.add(tid)
    elif payload.responsible_ids:
        rows = await fetch_with({"RESPONSIBLE_ID": payload.responsible_ids})
    else:
        rows = await fetch_with({})

    # Filtro local por DEADLINE (normalizando a UTC) cuando corresponde
    if date_key == "DEADLINE":
        def _deadline_utc(t: Dict[str, Any]) -> Optional[datetime]:
            d = _parse_deadline(t.get("DEADLINE"))
            if d is None:
                return None
            return _to_utc_aware(d)

        rows = [t for t in rows if (du := _deadline_utc(t)) and (from_utc <= du <= to_utc)]

    # Orden final por DEADLINE ascendente (fallback CREATED_DATE)
    def _sort_key(t: Dict[str, Any]):
        d = _parse_deadline(t.get("DEADLINE"))
        d = _to_utc_aware(d) if d else None
        if d is None:
            cd = t.get("CREATED_DATE")
            try:
                cd = _to_utc_aware(datetime.fromisoformat(str(cd).replace("Z", "+00:00")))
            except Exception:
                cd = datetime.max.replace(tzinfo=timezone.utc)
            return (datetime.max.replace(tzinfo=timezone.utc), cd)
        return (d, datetime.max.replace(tzinfo=timezone.utc))

    rows.sort(key=_sort_key)
    return {"items": rows, "count": len(rows)}
# ==========================================================


@app.post("/calendar")
async def list_calendar(payload: CalendarIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)
    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now

    owner_ids = payload.owner_ids or []
    if not owner_ids:
        return {"items": [], "count": 0}

    all_events: List[Dict[str, Any]] = []
    for uid in owner_ids:
        params = {
            "type": "user",
            "ownerId": uid,
            "from": _dtstr(from_dt),
            "to": _dtstr(to_dt),
            "select": ["ID", "OWNER_ID", "DATE_FROM", "DATE_TO", "NAME", "DESCRIPTION", "LOCATION"]
        }
        data = await bitrix_call("calendar.event.get", params)
        events = data.get("result") or []
        all_events.extend(events)

    return {"items": all_events, "count": len(all_events)}
