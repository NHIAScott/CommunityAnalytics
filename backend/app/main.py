from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.db.database import init_db

app = FastAPI(title="NHIA Community Analytics")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    init_db()


app.include_router(router)


@app.get("/health")
def health():
    return {"status": "ok"}
