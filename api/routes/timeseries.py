from fastapi import APIRouter, Query
from typing import Optional
import os, psycopg2, psycopg2.extras

router = APIRouter()
DB_URL = os.getenv("DATABASE_URL", "postgresql://opentrade:opentrade@localhost:5432/opentrade")

@router.get("/timeseries")
def timeseries(
    reporter:  str           = Query(...),
    partner:   Optional[str] = Query(None),
    hs_code:   Optional[str] = Query(None),
    flow:      Optional[str] = Query(None),
    year_from: int           = Query(2010),
    year_to:   int           = Query(2023),
):
    conn = psycopg2.connect(DB_URL)
    conds, params = ["reporter = %s", "year BETWEEN %s AND %s"], [reporter.upper(), year_from, year_to]
    if partner: conds.append("partner = %s"); params.append(partner.upper())
    if hs_code: conds.append("hs_code LIKE %s"); params.append(hs_code + "%")
    if flow:    conds.append("flow = %s"); params.append(flow.lower())
    sql = f"""
        SELECT year, flow, SUM(value_usd) AS value_usd, COUNT(*) AS num_records
        FROM trade_flows WHERE {" AND ".join(conds)}
        GROUP BY year, flow ORDER BY year, flow
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"reporter": reporter, "partner": partner or "WLD", "hs_code": hs_code or "ALL", "data": rows}
