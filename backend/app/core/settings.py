from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "NHIA Community Analytics"
    db_path: str = "backend/data/analytics.duckdb"
    raw_upload_dir: str = "backend/data/raw_uploads"
    timezone: str = "America/New_York"
    auth_password: str = ""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    @property
    def db_url(self) -> str:
        db = Path(self.db_path)
        db.parent.mkdir(parents=True, exist_ok=True)
        return f"duckdb:///{db.resolve()}"


settings = Settings()
