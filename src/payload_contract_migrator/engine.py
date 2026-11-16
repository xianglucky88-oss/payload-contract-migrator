def calculate(rows, payload):
    migrated=[]; quarantined=[]
    for row in rows:
        try:
            amount=float(row["amount"]); migrated.append({"id":row["id"],"schema":payload.get("target_schema","v2"),"full_name":row["name"],"amount":amount})
        except (TypeError, ValueError): quarantined.append({"id":row["id"],"reason":"amount_not_numeric","raw":row})
    return {"migrated":migrated,"quarantined":quarantined,"replayable":True}
