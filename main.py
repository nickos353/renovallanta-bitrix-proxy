import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

BITRIX_BASE_URL = (os.getenv("BITRIX_BASE_URL") or "").rstrip("/") + "/"
API_KEY = os.getenv("API_KEY")

app = FastAPI(title="Renovallanta Bitrix Aggregator", version="1.0.0")

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

class ActivitiesIn(BaseModel):
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    responsible_ids: Optional[List[int]] = None
    completed: Optional[str] = None    # "Y" | "N"
    types: Optional[List[str]] = None  # CALL, MEETING, EMAIL, TASK

class TasksIn(BaseModel):
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    responsible_ids: Optional[List[int]] = None
    status: Optional[List[int]] = None

class CalendarIn(BaseModel):
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    owner_ids: Optional[List[int]] = None

async def ensure_api_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida")

@app.get("/")
async def root():
    return {"ok": True, "service": "renovallanta-bitrix-proxy"}

@app.get("/users")
async def list_users(x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)
    params = {"filter": {"ACTIVE": True}, "select": ["ID","NAME","LAST_NAME","WORK_POSITION","DEPARTMENT"]}
    rows = await bitrix_fetch_all("user.get", params)
    return {"items": rows, "count": len(rows)}

@app.post("/activities")
async def list_activities(payload: ActivitiesIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)
    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt   = payload.to_dt   or  now
    filt: Dict[str, Any] = {
        ">=CREATED": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "<=CREATED": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    if payload.responsible_ids:
        filt["RESPONSIBLE_ID"] = payload.responsible_ids
    if payload.completed in ("Y","N"):
        filt["COMPLETED"] = payload.completed
    if payload.types:
        filt["TYPE_ID"] = payload.types
    params = {
        "filter": filt,
        "select": ["ID","TYPE_ID","SUBJECT","CREATED","RESPONSIBLE_ID","COMPLETED","DESCRIPTION","BINDINGS","DEADLINE","AUTHOR_ID","END_TIME"],
        "order": {"CREATED": "DESC"}
    }
    rows = await bitrix_fetch_all("crm.activity.list", params)
    return {"items": rows, "count": len(rows)}

@app.post("/tasks")
async def list_tasks(payload: TasksIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)
    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt   = payload.to_dt   or  now
    filt: Dict[str, Any] = {
        ">=CREATED_DATE": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "<=CREATED_DATE": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    if payload.responsible_ids:
        filt["RESPONSIBLE_ID"] = payload.responsible_ids
    if payload.status:
        filt["STATUS"] = payload.status
    params = {
        "filter": filt,
        "select": ["ID","TITLE","STATUS","RESPONSIBLE_ID","CREATED_DATE","CLOSED_DATE","DEADLINE","UF_CRM_TASK","GROUP_ID","PRIORITY"],
        "order": {"CREATED_DATE":"DESC"}
    }
    rows = await bitrix_fetch_all("tasks.task.list", params)
    return {"items": rows, "count": len(rows)}

@app.post("/calendar")
async def list_calendar(payload: CalendarIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)
    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt   = payload.to_dt   or  now
    owner_ids = payload.owner_ids or []
    if not owner_ids:
        return {"items": [], "count": 0}
    all_events: List[Dict[str, Any]] = []
    for uid in owner_ids:
        params = {
            "type": "user",
            "ownerId": uid,
            "from": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "to":   to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "select": ["ID","OWNER_ID","DATE_FROM","DATE_TO","NAME","DESCRIPTION","LOCATION"]
        }
        data = await bitrix_call("calendar.event.get", params)
        events = data.get("result") or []
        all_events.extend(events)
    return {"items": all_events, "count": len(all_events)}
