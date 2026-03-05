from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from fastapi import UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import settings
from app.etl.mappers import canonicalize_company, detect_dataset, normalize_col


def _clean(v: object) -> str | None:
    if v is None:
        return None
    txt = str(v).strip()
    if txt == "" or txt.lower() in {"nan", "none", "null"}:
        return None
    return txt


def _to_bool(v: object, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    txt = str(v).strip().lower()
    if txt in {"1", "true", "yes", "y", "t"}:
        return True
    if txt in {"0", "false", "no", "n", "f"}:
        return False
    return default


def _to_int(v: object) -> int:
    if v is None:
        return 0
    try:
        if pd.isna(v):
            return 0
    except Exception:
        pass
    try:
        return int(float(v))
    except Exception:
        return 0


def _user_key(df: pd.DataFrame) -> pd.Series:
    keys = pd.Series([None] * len(df), index=df.index, dtype="object")

    for col in ["integration_id", "contact_key", "email"]:
        if col in df.columns:
            vals = df[col].apply(_clean)
            keys = keys.where(keys.notna(), vals)

    fallback = (
        df.get("first_name", "").astype(str)
        + "|"
        + df.get("last_name", "").astype(str)
        + "|"
        + df.get("company_name", "").astype(str)
    ).apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    return keys.fillna(fallback).astype(str)


def ingest_uploads(files: list[UploadFile], db: Session, force: bool = False) -> list[dict]:
    results: list[dict] = []
    raw_dir = Path(settings.raw_upload_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    for file in files:
        content = file.file.read()
        file_hash = hashlib.sha256(content).hexdigest()
        existing = db.execute(text("SELECT 1 FROM ingestion_log WHERE file_hash=:h"), {"h": file_hash}).first()
        if existing and not force:
            results.append({"file": file.filename, "status": "duplicate_skipped"})
            continue

        ingestion_id = str(uuid4())
        save_path = raw_dir / f"{ingestion_id}_{file.filename}"
        save_path.write_bytes(content)

        rows = 0
        detected = []
        errors = []

        workbook = pd.read_excel(save_path, sheet_name=None)
        for _, df in workbook.items():
            if df.empty:
                continue
            df.columns = [normalize_col(str(c)) for c in df.columns]
            dataset = detect_dataset(df.columns.tolist())
            detected.append(dataset)

            if dataset in {"individual_user_engagement", "subscriber_discussion_activity", "active_member_accounts"}:
                u = df.copy()
                u["user_id"] = _user_key(u)
                if "company_name" in u.columns:
                    u["company_name"] = u["company_name"].astype(str)
                    u["company_canonical"] = u["company_name"].apply(canonicalize_company)
                _upsert_users(u, db)
                _insert_user_snapshot(u, ingestion_id, db)
                rows += len(u)
            elif dataset == "all_discussions":
                _insert_threads(df.copy(), ingestion_id, db)
                rows += len(df)
            elif dataset == "friend_requests":
                _insert_friend_requests(df.copy(), ingestion_id, db)
                rows += len(df)
            elif dataset == "logged_in_since_2024":
                u = df.copy()
                u["user_id"] = _user_key(u)
                if "company_name" in u.columns:
                    u["company_name"] = u["company_name"].astype(str)
                    u["company_canonical"] = u["company_name"].apply(canonicalize_company)
                _upsert_users(u, db)
                _insert_login_flags(u, ingestion_id, db)
                rows += len(df)
            else:
                errors.append(f"Unknown or unsupported sheet in {file.filename}")

        db.execute(
            text(
                """
                INSERT INTO ingestion_log (ingestion_id, uploaded_at, file_name, file_hash, detected_dataset, rows_ingested, status, errors)
                VALUES (:id, :ts, :fn, :fh, :dd, :rows, :status, :errors)
                """
            ),
            {
                "id": ingestion_id,
                "ts": datetime.utcnow(),
                "fn": file.filename,
                "fh": file_hash,
                "dd": ",".join(sorted(set(detected))),
                "rows": rows,
                "status": "success" if not errors else "partial_success",
                "errors": json.dumps(errors),
            },
        )
        db.commit()
        results.append({"file": file.filename, "ingestion_id": ingestion_id, "rows": rows, "errors": errors})

    return results


def _upsert_users(df: pd.DataFrame, db: Session) -> None:
    for _, row in df.iterrows():
        company_raw = _clean(row.get("company_name")) or "unknown"
        company_canonical = canonicalize_company(_clean(row.get("company_canonical")) or company_raw)
        company_id = hashlib.md5(company_canonical.encode()).hexdigest()[:12]
        db.execute(
            text("INSERT OR IGNORE INTO dim_company (company_id, company_name_canonical) VALUES (:id,:n)"),
            {"id": company_id, "n": company_canonical},
        )
        db.execute(
            text("INSERT INTO dim_company_variant (company_id, company_name_raw) VALUES (:id,:raw)"),
            {"id": company_id, "raw": company_raw},
        )
        db.execute(
            text(
                """
                INSERT OR REPLACE INTO dim_user (user_id, contact_key, first_name, last_name, email, company_name_raw, company_id,
                member_status, user_status, state, country, has_photo, has_bio, has_education, has_job_history,
                mentor_status, mentee_status, volunteer_status, last_seen_at)
                VALUES (:user_id,:contact_key,:first_name,:last_name,:email,:company_name_raw,:company_id,
                :member_status,:user_status,:state,:country,:has_photo,:has_bio,:has_education,:has_job_history,
                :mentor_status,:mentee_status,:volunteer_status,:last_seen_at)
                """
            ),
            {
                "user_id": _clean(row.get("user_id")),
                "contact_key": _clean(row.get("contact_key")),
                "first_name": _clean(row.get("first_name")),
                "last_name": _clean(row.get("last_name")),
                "email": _clean(row.get("email")),
                "company_name_raw": company_raw,
                "company_id": company_id,
                "member_status": _clean(row.get("member_status")),
                "user_status": _clean(row.get("user_status")),
                "state": _clean(row.get("state")),
                "country": _clean(row.get("country")),
                "has_photo": _to_bool(row.get("has_photo"), False),
                "has_bio": _to_bool(row.get("has_bio"), False),
                "has_education": _to_bool(row.get("has_education"), False),
                "has_job_history": _to_bool(row.get("has_job_history"), False),
                "mentor_status": _to_bool(row.get("mentor_status"), False),
                "mentee_status": _to_bool(row.get("mentee_status"), False),
                "volunteer_status": _to_bool(row.get("volunteer_status"), False),
                "last_seen_at": datetime.utcnow(),
            },
        )


def _insert_user_snapshot(df: pd.DataFrame, ingestion_id: str, db: Session) -> None:
    now = datetime.utcnow().date()
    for _, row in df.iterrows():
        db.execute(
            text(
                """
                INSERT INTO fact_user_activity_snapshot
                (period_start_date, period_grain, user_id, logins, downloads, documents_created, threads_created,
                discussion_replies, replies_to_sender, blogs_created, questions_created, answers_created,
                best_answers_discussion, best_answers_qa, recommends_given, follows, source_upload_id)
                VALUES (:d,'week',:user_id,:logins,:downloads,:documents_created,:threads_created,:discussion_replies,
                :replies_to_sender,:blogs_created,:questions_created,:answers_created,:bad,:baq,:rec,:f,:src)
                """
            ),
            {
                "d": now,
                "user_id": _clean(row.get("user_id")),
                "logins": _to_int(row.get("logins")),
                "downloads": _to_int(row.get("downloads")),
                "documents_created": _to_int(row.get("documents_created")),
                "threads_created": _to_int(row.get("threads_created")),
                "discussion_replies": _to_int(row.get("discussion_replies")),
                "replies_to_sender": _to_int(row.get("replies_to_sender")),
                "blogs_created": _to_int(row.get("blogs_created")),
                "questions_created": _to_int(row.get("questions_created")),
                "answers_created": _to_int(row.get("answers_created")),
                "bad": _to_int(row.get("best_answers_discussion")),
                "baq": _to_int(row.get("best_answers_qa")),
                "rec": _to_int(row.get("recommends_given")),
                "f": _to_int(row.get("follows")),
                "src": ingestion_id,
            },
        )


def _insert_threads(df: pd.DataFrame, ingestion_id: str, db: Session) -> None:
    for _, row in df.iterrows():
        sid = str(row.get("thread_id", ""))
        if not sid or sid == "nan":
            sid = hashlib.md5(f"{row.get('subject','')}|{row.get('created_at','')}|{row.get('author','')}".encode()).hexdigest()
        db.execute(
            text(
                """
                INSERT OR REPLACE INTO dim_thread (thread_id, community_name, community_type, thread_type, subject,
                created_at, closed_at, author_user_id, total_replies, replies_to_thread, replies_to_sender,
                total_recommends, total_following, source_upload_id)
                VALUES (:id,:c,:ct,:tt,:s,:ca,:cl,:au,:tr,:rtt,:rts,:trec,:tf,:src)
                """
            ),
            {
                "id": sid,
                "c": row.get("community_name"),
                "ct": row.get("community_type"),
                "tt": row.get("thread_type"),
                "s": row.get("subject"),
                "ca": row.get("created_at"),
                "cl": row.get("closed_at"),
                "au": row.get("author_user_id") or row.get("author"),
                "tr": _to_int(row.get("total_replies")),
                "rtt": _to_int(row.get("replies_to_thread")),
                "rts": _to_int(row.get("replies_to_sender")),
                "trec": _to_int(row.get("total_recommends")),
                "tf": _to_int(row.get("total_following")),
                "src": ingestion_id,
            },
        )


def _insert_friend_requests(df: pd.DataFrame, ingestion_id: str, db: Session) -> None:
    for _, row in df.iterrows():
        db.execute(
            text(
                """
                INSERT INTO fact_friend_requests (requester_user_id, requested_user_id, request_status, request_date, source_upload_id)
                VALUES (:r,:q,:s,:d,:src)
                """
            ),
            {
                "r": _clean(row.get("requester_user_id") or row.get("requester")),
                "q": _clean(row.get("requested_user_id") or row.get("requested")),
                "s": _clean(row.get("request_status")) or "unknown",
                "d": row.get("request_date"),
                "src": ingestion_id,
            },
        )


def _insert_login_flags(df: pd.DataFrame, ingestion_id: str, db: Session) -> None:
    for _, row in df.iterrows():
        uid = _clean(row.get("user_id") or row.get("integration_id") or row.get("contact_key") or row.get("email"))
        if not uid:
            continue
        db.execute(
            text(
                """
                INSERT INTO fact_login_flag (user_id, logged_in_since_2024, last_login_date, source_upload_id)
                VALUES (:u,:f,:d,:src)
                """
            ),
            {
                "u": uid,
                "f": _to_bool(row.get("logged_in_since_2024"), True),
                "d": row.get("last_login_date"),
                "src": ingestion_id,
            },
        )
