from fastapi import FastAPI
from .routers import fighters, events  # Relative import
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

from fastapi import FastAPI
from .routers import fighters, events  # Relative import

app = FastAPI(
    title="UFC API",
    version="1.0.0",
    openapi_url="/openapi.json"
)

app.include_router(fighters.router)
app.include_router(events.router)

@app.get("/")
def health_check():
    return {"status": "running", "version": app.version}