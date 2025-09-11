@app.post("/tasks")
async def list_tasks(payload: TasksIn, x_api_key: Optional[str] = Header(default=None)):
    await ensure_api_key(x_api_key)

    now = datetime.utcnow()
    from_dt = payload.from_dt or (now - timedelta(days=7))
    to_dt = payload.to_dt or now

    # Normalizamos rango a UTC-aware
    def _to_utc_aware(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    from_utc = _to_utc_aware(from_dt)
    to_utc = _to_utc_aware(to_dt)

    date_key = (payload.date_field or "DEADLINE").upper()
    if date_key not in {"DEADLINE", "CREATED_DATE", "CLOSED_DATE"}:
        date_key = "DEADLINE"

    # Filtro hacia Bitrix (defensivo cuando pedimos DEADLINE)
    base_filter: Dict[str, Any] = {}
    if date_key == "DEADLINE":
        wide_from = from_dt - timedelta(days=21)
        wide_to = to_dt + timedelta(days=7)
        base_filter[">=CREATED_DATE"] = wide_from.strftime("%Y-%m-%dT%H:%M:%S")
        base_filter["<=CREATED_DATE"] = wide_to.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        base_filter[f">={date_key}"] = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
        base_filter[f"<={date_key}"] = to_dt.strftime("%Y-%m-%dT%H:%M:%S")

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
    # Si llegan ambos filtros, unimos resultados (Bitrix a veces no acepta MEMBER+GROUP_ID a la vez)
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

    # ---- Helpers de parseo (aceptan mayúsculas/minúsculas) ----
    def _parse_deadline(val: Any) -> Optional[datetime]:
        if not val:
            return None
        s = str(val).strip()
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

    def _deadline_raw(t: Dict[str, Any]) -> Any:
        return t.get("DEADLINE") or t.get("deadline") or t.get("Deadline")

    def _created_raw(t: Dict[str, Any]) -> Any:
        return t.get("CREATED_DATE") or t.get("createdDate") or t.get("created_date") or t.get("CREATED")

    # Filtro local por DEADLINE (normalizando a UTC) cuando corresponde
    if date_key == "DEADLINE":
        def _deadline_utc(t: Dict[str, Any]) -> Optional[datetime]:
            d = _parse_deadline(_deadline_raw(t))
            return _to_utc_aware(d) if d else None

        rows = [t for t in rows if (du := _deadline_utc(t)) and (from_utc <= du <= to_utc)]

    # Orden final por DEADLINE ascendente (fallback CREATED_DATE)
    def _sort_key(t: Dict[str, Any]):
        d = _parse_deadline(_deadline_raw(t))
        d = _to_utc_aware(d) if d else None
        if d is None:
            cd = _created_raw(t)
            try:
                cd = datetime.fromisoformat(str(cd).replace("Z", "+00:00"))
                cd = _to_utc_aware(cd)
            except Exception:
                cd = datetime.max.replace(tzinfo=timezone.utc)
            return (datetime.max.replace(tzinfo=timezone.utc), cd)
        return (d, datetime.max.replace(tzinfo=timezone.utc))

    rows.sort(key=_sort_key)
    return {"items": rows, "count": len(rows)}
