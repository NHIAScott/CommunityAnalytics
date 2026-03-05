from app.etl.mappers import canonicalize_company, detect_dataset, normalize_col
from app.services.analytics_service import classify_tier


def test_normalize_col():
    assert normalize_col("Integration ID") == "integration_id"


def test_detect_dataset():
    cols = ["logins", "downloads", "discussion replies"]
    assert detect_dataset(cols) == "individual_user_engagement"


def test_canonicalize_company():
    assert canonicalize_company("Acme, Inc.") == "acme"


def test_tier_classification():
    assert classify_tier({"logins": 1, "downloads": 0, "discussion_replies": 0, "threads_created": 0, "blogs_created": 0, "best_answers": 0, "super_pct": 0, "pagerank_pct": 0}) == "Observer"
    assert classify_tier({"logins": 1, "downloads": 2, "discussion_replies": 0, "threads_created": 0, "blogs_created": 0, "best_answers": 0, "super_pct": 0, "pagerank_pct": 0}) == "Consumer"
    assert classify_tier({"logins": 1, "downloads": 0, "discussion_replies": 1, "threads_created": 0, "blogs_created": 0, "best_answers": 0, "super_pct": 0, "pagerank_pct": 0}) == "Participant"
