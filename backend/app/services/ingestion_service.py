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


def _user_key(df: pd.DataFrame) -> pd.Series:
    for col in ["integration_id", "contact_key", "email"]:
        if col in df.columns:
            v = df[col].astype(str).str.strip()
            if v.notna().any():
                return v
    return (
        df.get("first_name", "").astype(str)
        + "|"
        + df.get("last_name", "").astype(str)
        + "|"
        + df.get("company_name", "").astype(str)
    ).apply(lambda x: hashlib.md5(x.encode()).hexdigest())


def ingest_uploads(files: list[UploadFile], db: Session) -> list[dict]:
    results: list[dict] = []
    raw_dir = Path(settings.raw_upload_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    for file in files:
        content = file.file.read()
        file_hash = hashlib.sha256(content).hexdigest()
        existing = db.execute(text("SELECT 1 FROM ingestion_log WHERE file_hash=:h"), {"h": file_hash}).first()
        if existing:
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
                _insert_login_flags(df.copy(), ingestion_id, db)
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
        company_id = hashlib.md5(str(row.get("company_canonical", "unknown")).encode()).hexdigest()[:12]
        db.execute(
            text("INSERT OR IGNORE INTO dim_company (company_id, company_name_canonical) VALUES (:id,:n)"),
            {"id": company_id, "n": row.get("company_canonical", "unknown")},
        )
        db.execute(
            text("INSERT INTO dim_company_variant (company_id, company_name_raw) VALUES (:id,:raw)"),
            {"id": company_id, "raw": row.get("company_name", "")},
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
                "user_id": row.get("user_id"),
                "contact_key": row.get("contact_key"),
                "first_name": row.get("first_name"),
                "last_name": row.get("last_name"),
                "email": row.get("email"),
                "company_name_raw": row.get("company_name"),
                "company_id": company_id,
                "member_status": row.get("member_status"),
                "user_status": row.get("user_status"),
                "state": row.get("state"),
                "country": row.get("country"),
                "has_photo": bool(row.get("has_photo", False)),
                "has_bio": bool(row.get("has_bio", False)),
                "has_education": bool(row.get("has_education", False)),
                "has_job_history": bool(row.get("has_job_history", False)),
                "mentor_status": bool(row.get("mentor_status", False)),
                "mentee_status": bool(row.get("mentee_status", False)),
                "volunteer_status": bool(row.get("volunteer_status", False)),
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
                "user_id": row.get("user_id"),
                "logins": int(row.get("logins", 0) or 0),
                "downloads": int(row.get("downloads", 0) or 0),
                "documents_created": int(row.get("documents_created", 0) or 0),
                "threads_created": int(row.get("threads_created", 0) or 0),
                "discussion_replies": int(row.get("discussion_replies", 0) or 0),
                "replies_to_sender": int(row.get("replies_to_sender", 0) or 0),
                "blogs_created": int(row.get("blogs_created", 0) or 0),
                "questions_created": int(row.get("questions_created", 0) or 0),
                "answers_created": int(row.get("answers_created", 0) or 0),
                "bad": int(row.get("best_answers_discussion", 0) or 0),
                "baq": int(row.get("best_answers_qa", 0) or 0),
                "rec": int(row.get("recommends_given", 0) or 0),
                "f": int(row.get("follows", 0) or 0),
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
                "tr": int(row.get("total_replies", 0) or 0),
                "rtt": int(row.get("replies_to_thread", 0) or 0),
                "rts": int(row.get("replies_to_sender", 0) or 0),
                "trec": int(row.get("total_recommends", 0) or 0),
                "tf": int(row.get("total_following", 0) or 0),
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
                "r": row.get("requester_user_id") or row.get("requester"),
                "q": row.get("requested_user_id") or row.get("requested"),
                "s": row.get("request_status", "unknown"),
                "d": row.get("request_date"),
                "src": ingestion_id,
            },
        )


def _insert_login_flags(df: pd.DataFrame, ingestion_id: str, db: Session) -> None:
    for _, row in df.iterrows():
        uid = row.get("integration_id") or row.get("contact_key") or row.get("email")
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
                "u": str(uid),
                "f": bool(row.get("logged_in_since_2024", True)),
                "d": row.get("last_login_date"),
                "src": ingestion_id,
            },
        )
