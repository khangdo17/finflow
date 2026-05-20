"""
FinFlow FastAPI application entry point.
Mounts all four routers and configures CORS to allow the Streamlit dashboard
running on localhost:8501 to make cross-origin requests.
Run with: uvicorn serving.api.main:app --host 0.0.0.0 --port 8000
"""
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from serving.api.routes import fraud, health, observability, revenue

load_dotenv()

app = FastAPI(
    title="FinFlow API",
    description="Real-time fraud detection and revenue analytics for FinFlow Lambda Architecture",
    version="1.0.0",
)

# Allow Streamlit dashboard to call the API cross-origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(fraud.router,         prefix="/fraud",         tags=["fraud"])
app.include_router(revenue.router,       prefix="/revenue",       tags=["revenue"])
app.include_router(health.router,        prefix="/health",        tags=["health"])
app.include_router(observability.router, prefix="/observability", tags=["observability"])


@app.on_event("startup")
async def on_startup() -> None:
    logger.info(
        f"FinFlow API starting on port {os.getenv('API_PORT', '8000')}"
    )
