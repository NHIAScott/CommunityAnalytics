from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.analytics_service import run_materializations
from app.services.ingestion_service import ingest_uploads

router = APIRouter(prefix="/api")


@router.get("/ingestions")
def list_ingestions(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT * FROM ingestion_log ORDER BY uploaded_at DESC LIMIT 200")).mappings().all()
    return [dict(r) for r in rows]


@router.post("/ingestions")
def upload_ingestions(files: list[UploadFile] = File(...), force: bool = False, db: Session = Depends(get_db)):
    result = ingest_uploads(files, db, force=force)
    materialization = run_materializations(db)
    return {"result": result, "materialization": materialization}


@router.get("/overview")
def overview(db: Session = Depends(get_db)):
    kpis = db.execute(
        text(
            """
            SELECT
              (SELECT COALESCE(COUNT(DISTINCT user_id),0) FROM fact_login_flag WHERE logged_in_since_2024 = true) total_members,
              (SELECT COUNT(DISTINCT user_id) FROM fact_user_activity_snapshot WHERE logins>0) active_users,
              (SELECT COUNT(DISTINCT user_id) FROM fact_user_activity_snapshot WHERE (logins+downloads+discussion_replies+threads_created+blogs_created)>0) engaged_users,
              (SELECT COUNT(DISTINCT user_id) FROM fact_user_activity_snapshot WHERE (threads_created+blogs_created)>0) contributors,
              (SELECT COALESCE(SUM(threads_created),0) FROM fact_user_activity_snapshot) total_threads,
              (SELECT COALESCE(SUM(discussion_replies),0) FROM fact_user_activity_snapshot) total_replies,
              (SELECT COALESCE(SUM(downloads),0) FROM fact_user_activity_snapshot) downloads
            """
        )
    ).mappings().first()
    trends = db.execute(text("SELECT period_start_date, SUM(logins) logins, SUM(downloads) downloads, SUM(threads_created) threads, SUM(discussion_replies) replies FROM fact_user_activity_snapshot GROUP BY 1 ORDER BY 1")).mappings().all()
    return {"kpis": dict(kpis or {}), "trends": [dict(r) for r in trends]}


@router.get("/users")
def users(db: Session = Depends(get_db), company_id: str | None = None, tier: str | None = None):
    q = "SELECT u.user_id, u.first_name, u.last_name, u.company_id, COALESCE(s.engagement_score_0_100,0) engagement_score_0_100, COALESCE(s.super_user_score_0_100,0) super_user_score_0_100, COALESCE(s.engagement_tier,'Observer') engagement_tier FROM dim_user u LEFT JOIN mart_user_scores_period s ON s.user_id=u.user_id WHERE 1=1"
    params = {}
    if company_id:
        q += " AND u.company_id=:company_id"
        params["company_id"] = company_id
    if tier:
        q += " AND s.engagement_tier=:tier"
        params["tier"] = tier
    rows = db.execute(text(q + " ORDER BY s.engagement_score_0_100 DESC NULLS LAST LIMIT 500"), params).mappings().all()
    return [dict(r) for r in rows]


@router.get("/companies")
def companies(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT c.company_id, c.company_name_canonical, COALESCE(h.company_health_score_0_100,0) company_health_score_0_100, COALESCE(h.active_users,0) active_users, COALESCE(h.engaged_users,0) engaged_users, COALESCE(h.risk_flags_json,'[]') risk_flags_json FROM dim_company c LEFT JOIN mart_company_health_period h ON c.company_id=h.company_id ORDER BY company_health_score_0_100 DESC")).mappings().all()
    return [dict(r) for r in rows]


@router.get("/topics")
def topics(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT c.topic_id, c.topic_label, c.top_keywords_json, m.threads, m.replies, m.influence_score FROM mart_topic_catalog c LEFT JOIN mart_topic_metrics_period m ON c.topic_id=m.topic_id ORDER BY m.influence_score DESC NULLS LAST")).mappings().all()
    return [dict(r) for r in rows]


@router.get("/network")
def network(db: Session = Depends(get_db)):
    edges = db.execute(text("SELECT * FROM mart_network_edges_period LIMIT 1000")).mappings().all()
    metrics = db.execute(text("SELECT * FROM mart_network_metrics_user_period ORDER BY pagerank DESC LIMIT 200")).mappings().all()
    return {"edges": [dict(e) for e in edges], "metrics": [dict(m) for m in metrics]}


@router.get("/exports")
def exports(db: Session = Depends(get_db), dataset: str = "users"):
    mapping = {
        "users": "SELECT * FROM mart_user_scores_period",
        "companies": "SELECT * FROM mart_company_health_period",
        "topics": "SELECT * FROM mart_topic_metrics_period",
    }
    rows = db.execute(text(mapping.get(dataset, mapping["users"]))).mappings().all()
    return [dict(r) for r in rows]
