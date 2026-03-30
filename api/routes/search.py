from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Optional
import os, psycopg2, psycopg2.extras

router = APIRouter()
DB_URL = os.getenv("DATABASE_URL", "postgresql://opentrade:opentrade@localhost:5432/opentrade")

class SearchRequest(BaseModel):
    q:          str            = Field(..., description="e.g. 'solar panels from China to Germany'")
    top_k:      int            = Field(10, ge=1, le=100)
    reporter:   Optional[str]  = None
    partner:    Optional[str]  = None
    hs_code:    Optional[str]  = None
    year_start: Optional[int]  = None
    year_end:   Optional[int]  = None
    flow:       Optional[str]  = None

@router.post("/search")
def search(req: SearchRequest):
    conn = psycopg2.connect(DB_URL)
    conds, params = ["1=1"], []
    if req.reporter: conds.append("tf.reporter = %s"); params.append(req.reporter.upper())
    if req.partner:  conds.append("tf.partner = %s");  params.append(req.partner.upper())
    if req.hs_code:  conds.append("tf.hs_code LIKE %s"); params.append(req.hs_code + "%")
    if req.year_start: conds.append("tf.year >= %s"); params.append(req.year_start)
    if req.year_end:   conds.append("tf.year <= %s"); params.append(req.year_end)
    if req.flow:     conds.append("tf.flow = %s"); params.append(req.flow.lower())
    if req.q:
        conds.append("(p.description ILIKE %s OR tf.reporter ILIKE %s OR tf.hs_code ILIKE %s)")
        kw = f"%{req.q}%"; params.extend([kw, kw, kw])
    sql = f"""
        SELECT tf.reporter, c_rep.name AS reporter_name, tf.partner, c_par.name AS partner_name,
               tf.hs_code, p.description AS product, tf.year, tf.flow, tf.value_usd, tf.source
        FROM trade_flows tf
        LEFT JOIN countries c_rep ON c_rep.code = tf.reporter
        LEFT JOIN countries c_par ON c_par.code = tf.partner
        LEFT JOIN products  p     ON p.hs_code   = tf.hs_code
        WHERE {" AND ".join(conds)}
        ORDER BY tf.value_usd DESC NULLS LAST LIMIT %s
    """
    params.append(req.top_k)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        hits = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"query": req.q, "hits": hits, "total": len(hits)}
