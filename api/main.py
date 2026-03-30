"""OpenTrade API — FastAPI application"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from api.routes import search, timeseries, tariffs, countries, products

app = FastAPI(
    title="OpenTrade API",
    description="The open-source global trade intelligence API.",
    version="0.1.0",
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

app.include_router(search.router,     prefix="/v1")
app.include_router(timeseries.router, prefix="/v1")
app.include_router(tariffs.router,    prefix="/v1")
app.include_router(countries.router,  prefix="/v1")
app.include_router(products.router,   prefix="/v1")

FRONTEND = Path(__file__).parent.parent / "frontend"

@app.get("/")
def root():
    index = FRONTEND / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"name": "OpenTrade API", "version": "0.1.0", "docs": "/docs", "github": "https://github.com/gawiz93/opentrade"}

@app.get("/health")
def health():
    return {"status": "ok"}

# Serve static assets if any
if FRONTEND.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
