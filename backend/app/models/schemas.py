from pydantic import BaseModel


class IngestionResult(BaseModel):
    file: str
    status: str | None = None
    ingestion_id: str | None = None
    rows: int | None = None
    errors: list[str] | None = None
