from fastapi import APIRouter, Query
from typing import Optional
import os, psycopg2, psycopg2.extras

router = APIRouter()
DB_URL = os.getenv("DATABASE_URL", "postgresql://opentrade:opentrade@localhost:5432/opentrade")

@router.get("/products")
def list_products(
    q:        Optional[str] = Query(None, description="Search product description"),
    chapter:  Optional[str] = Query(None, description="HS chapter e.g. '84'"),
    hs_level: Optional[int] = Query(None, description="2, 4, or 6 digit"),
    limit:    int           = Query(50, le=200),
):
    conn = psycopg2.connect(DB_URL)
    conds, params = ["1=1"], []
    if q:        conds.append("description ILIKE %s"); params.append(f"%{q}%")
    if chapter:  conds.append("chapter = %s"); params.append(chapter)
    if hs_level: conds.append("hs_level = %s"); params.append(hs_level)
    sql = f"""
        SELECT hs_code, hs_level, description, section, chapter FROM products
        WHERE {" AND ".join(conds)} ORDER BY hs_code LIMIT %s
    """
    params.append(limit)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"products": rows, "total": len(rows)}
