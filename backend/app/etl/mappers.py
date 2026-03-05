from __future__ import annotations

import re
from typing import Dict

DATASET_SIGNATURES = {
    "individual_user_engagement": {"logins", "downloads", "discussion replies"},
    "logged_in_since_2024": {"logged in since 2024", "integration id"},
    "standard_profile_completion": {"profile completion", "has photo"},
    "subscriber_discussion_activity": {"replies", "threads"},
    "active_member_accounts": {"member status", "user status"},
    "all_discussions": {"subject", "total replies"},
    "friend_requests": {"requester", "requested"},
}

COLUMN_ALIASES: Dict[str, str] = {
    "integration id": "integration_id",
    "contact key": "contact_key",
    "email address": "email",
    "company": "company_name",
    "discussion replies": "discussion_replies",
    "threads created": "threads_created",
    "blogs created": "blogs_created",
    "best answers qa": "best_answers_qa",
    "best answers discussion": "best_answers_discussion",
    "total replies": "total_replies",
    "created": "created_at",
}


def normalize_col(col: str) -> str:
    col = re.sub(r"\s+", " ", col.strip().lower())
    return COLUMN_ALIASES.get(col, col.replace(" ", "_"))


def detect_dataset(columns: list[str]) -> str:
    normalized = set(c.lower().replace("_", " ") for c in columns)
    for dataset, signature in DATASET_SIGNATURES.items():
        if signature.issubset(normalized):
            return dataset
    return "unknown"


def canonicalize_company(name: str) -> str:
    if not name:
        return "unknown"
    txt = name.lower().strip()
    txt = re.sub(r"[.,]", "", txt)
    txt = re.sub(r"\b(inc|incorporated|llc|ltd|corp|corporation|co)\b", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt
