from fastapi import APIRouter, Query
from typing import Optional
import os, psycopg2, psycopg2.extras

router = APIRouter()
DB_URL = os.getenv("DATABASE_URL", "postgresql://opentrade:opentrade@localhost:5432/opentrade")

@router.get("/tariffs")
def get_tariffs(
    importer:    str           = Query(...),
    hs_code:     Optional[str] = Query(None),
    exporter:    Optional[str] = Query(None),
    year:        Optional[int] = Query(None),
    tariff_type: Optional[str] = Query(None),
):
    conn = psycopg2.connect(DB_URL)
    conds, params = ["importer = %s"], [importer.upper()]
    if hs_code:     conds.append("hs_code LIKE %s"); params.append(hs_code + "%")
    if exporter:    conds.append("exporter = %s"); params.append(exporter.upper())
    else:           conds.append("exporter IS NULL")
    if year:        conds.append("year = %s"); params.append(year)
    if tariff_type: conds.append("tariff_type = %s"); params.append(tariff_type)
    sql = f"""
        SELECT importer, exporter, hs_code, year, tariff_type, rate_pct, source
        FROM tariffs WHERE {" AND ".join(conds)}
        ORDER BY hs_code, year DESC LIMIT 500
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"importer": importer, "tariffs": rows, "total": len(rows)}
