"""UC Data Advisor — Databricks App entry point."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

app = FastAPI(title="UC Data Advisor")

# Initialize MLflow tracing (non-blocking — logs warning if MLflow unavailable)
try:
    from server.tracing import init_tracing
    init_tracing()
except Exception:
    pass

from server.routes.chat import router as chat_router
app.include_router(chat_router, prefix="/api")


@app.get("/api/health")
async def health():
    return {"status": "ok"}


# Serve static frontend
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(os.path.join(static_dir, "index.html"))
