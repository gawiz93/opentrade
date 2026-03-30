"""OpenTrade API — FastAPI application"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

@app.get("/")
def root():
    return {"name": "OpenTrade API", "version": "0.1.0", "docs": "/docs", "github": "https://github.com/gawiz93/opentrade"}

@app.get("/health")
def health():
    return {"status": "ok"}
