from fastapi import APIRouter, Query
from typing import Optional
import os, psycopg2, psycopg2.extras

router = APIRouter()
DB_URL = os.getenv("DATABASE_URL", "postgresql://opentrade:opentrade@localhost:5432/opentrade")

@router.get("/countries")
def list_countries():
    conn = psycopg2.connect(DB_URL)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT c.code, c.name, c.region, COUNT(tf.id) AS trade_records
            FROM countries c LEFT JOIN trade_flows tf ON tf.reporter = c.code
            GROUP BY c.code, c.name, c.region ORDER BY trade_records DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"countries": rows, "total": len(rows)}

@router.get("/partners")
def top_partners(
    reporter: str           = Query(...),
    flow:     Optional[str] = Query(None),
    year:     Optional[int] = Query(None),
    top_k:    int           = Query(20),
):
    conn = psycopg2.connect(DB_URL)
    conds, params = ["tf.reporter = %s", "tf.partner != 'WLD'"], [reporter.upper()]
    if flow: conds.append("tf.flow = %s"); params.append(flow.lower())
    if year: conds.append("tf.year = %s"); params.append(year)
    sql = f"""
        SELECT tf.partner AS code, c.name, tf.flow,
               SUM(tf.value_usd) AS total_value_usd, MAX(tf.year) AS latest_year
        FROM trade_flows tf LEFT JOIN countries c ON c.code = tf.partner
        WHERE {" AND ".join(conds)}
        GROUP BY tf.partner, c.name, tf.flow ORDER BY total_value_usd DESC NULLS LAST LIMIT %s
    """
    params.append(top_k)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"reporter": reporter, "partners": rows}
