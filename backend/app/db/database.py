from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core.settings import settings

engine = create_engine(settings.db_url, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    from app.db.schema import ddl_statements

    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))
