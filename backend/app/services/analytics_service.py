from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import networkx as nx
import numpy as np
import pandas as pd
import yaml
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy import text
from sqlalchemy.orm import Session


def _winsorize(series: pd.Series, p: float = 0.99) -> pd.Series:
    cap = series.quantile(p) if len(series) else 0
    return series.clip(upper=cap)


def _scale_0_100(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    lo, hi = float(series.min()), float(series.max())
    if hi == lo:
        return pd.Series(np.full(len(series), 50.0), index=series.index)
    return (series - lo) * 100.0 / (hi - lo)


def classify_tier(r: pd.Series, super_cutoff: float = 95, centrality_cutoff: float = 0.75) -> str:
    if (r.get("best_answers", 0) > 0) or (r.get("super_pct", 0) >= super_cutoff) or (r.get("pagerank_pct", 0) >= centrality_cutoff):
        return "Leader"
    if r.get("threads_created", 0) > 0 or r.get("blogs_created", 0) > 0:
        return "Contributor"
    if r.get("discussion_replies", 0) > 0:
        return "Participant"
    if r.get("downloads", 0) > 0:
        return "Consumer"
    if r.get("logins", 0) > 0:
        return "Observer"
    return "Observer"


def run_materializations(db: Session, config_path: str = "backend/config/scoring.yaml") -> dict:
    cfg = yaml.safe_load(Path(config_path).read_text())
    weights = cfg["engagement_weights"]

    users = pd.read_sql(
        text(
            """
            SELECT
              current_date as period_start_date,
              u.user_id,
              COALESCE(SUM(s.logins),0) logins,
              COALESCE(SUM(s.downloads),0) downloads,
              COALESCE(SUM(s.discussion_replies),0) discussion_replies,
              COALESCE(SUM(s.threads_created),0) threads_created,
              COALESCE(SUM(s.blogs_created),0) blogs_created,
              COALESCE(SUM(s.best_answers_discussion),0)+COALESCE(SUM(s.best_answers_qa),0) best_answers
            FROM dim_user u
            LEFT JOIN fact_user_activity_snapshot s ON s.user_id = u.user_id
            GROUP BY u.user_id
            """
        ),
        db.bind,
    )

    if users.empty:
        return {"status": "no_data"}

    friends = pd.read_sql(text("SELECT requester_user_id user_id, count(*) cnt FROM fact_friend_requests GROUP BY 1"), db.bind)
    users = users.merge(friends, on="user_id", how="left").fillna({"cnt": 0})

    users["eng_raw"] = (
        users.logins * weights["logins"]
        + users.downloads * weights["downloads"]
        + users.discussion_replies * weights["discussion_replies"]
        + users.threads_created * weights["threads_created"]
        + users.blogs_created * weights["blogs_created"]
        + users.best_answers * weights["best_answers"]
        + users.cnt * weights["friend_connections"]
    )
    users["eng_raw"] = _winsorize(users["eng_raw"]) 
    users["engagement_score"] = _scale_0_100(users["eng_raw"]).round(2)

    net = _build_network_metrics(db)
    users = users.merge(net[["user_id", "pagerank"]], on="user_id", how="left").fillna({"pagerank": 0})
    users["super_raw"] = users["eng_raw"] * 0.7 + users["pagerank"] * 100 * 0.3
    users["super_raw"] = _winsorize(users["super_raw"])
    users["super_score"] = _scale_0_100(users["super_raw"]).round(2)
    users["super_pct"] = users["super_score"].rank(pct=True) * 100
    users["pagerank_pct"] = users["pagerank"].rank(pct=True)

    users["tier"] = users.apply(classify_tier, axis=1)
    users["drivers_json"] = users.apply(
        lambda r: json.dumps(
            sorted(
                {
                    "threads_created": int(r["threads_created"]),
                    "discussion_replies": int(r["discussion_replies"]),
                    "best_answers": int(r["best_answers"]),
                    "downloads": int(r["downloads"]),
                }.items(),
                key=lambda kv: kv[1],
                reverse=True,
            )[:3]
        ),
        axis=1,
    )

    db.execute(text("DELETE FROM mart_user_scores_period"))
    for _, r in users.iterrows():
        db.execute(
            text(
                """
                INSERT INTO mart_user_scores_period (period_start_date, grain, user_id, engagement_score_0_100, super_user_score_0_100, engagement_tier, drivers_json)
                VALUES (:d,'week',:u,:e,:s,:t,:dr)
                """
            ),
            {"d": r["period_start_date"], "u": r["user_id"], "e": r["engagement_score"], "s": r["super_score"], "t": r["tier"], "dr": r["drivers_json"]},
        )

    _build_company_health(db)
    _build_topics(db, cfg)
    run_id = str(uuid4())
    db.execute(
        text("INSERT INTO run_metadata (run_id, executed_at, scoring_config_json, counts_json) VALUES (:id,:at,:cfg,:c)"),
        {"id": run_id, "at": datetime.utcnow(), "cfg": json.dumps(cfg), "c": json.dumps({"users": len(users)})},
    )
    db.commit()
    return {"status": "ok", "run_id": run_id}


def _build_network_metrics(db: Session) -> pd.DataFrame:
    edges = pd.read_sql(text("SELECT requester_user_id as from_user_id, requested_user_id as to_user_id FROM fact_friend_requests"), db.bind)
    if edges.empty:
        return pd.DataFrame(columns=["user_id", "pagerank"])
    G = nx.DiGraph()
    for _, e in edges.iterrows():
        G.add_edge(str(e["from_user_id"]), str(e["to_user_id"]), weight=1)
    pr = nx.pagerank(G)
    bet = nx.betweenness_centrality(G)

    db.execute(text("DELETE FROM mart_network_metrics_user_period"))
    db.execute(text("DELETE FROM mart_network_edges_period"))
    today = datetime.utcnow().date()
    for u in G.nodes:
        db.execute(
            text(
                "INSERT INTO mart_network_metrics_user_period (period_start_date, grain, user_id, in_degree, out_degree, pagerank, betweenness, reciprocity_rate) VALUES (:d,'week',:u,:i,:o,:p,:b,:r)"
            ),
            {
                "d": today,
                "u": u,
                "i": G.in_degree(u),
                "o": G.out_degree(u),
                "p": pr.get(u, 0),
                "b": bet.get(u, 0),
                "r": nx.reciprocity(G, u) or 0,
            },
        )
    for a, b in G.edges:
        db.execute(
            text("INSERT INTO mart_network_edges_period (period_start_date, grain, from_user_id, to_user_id, weight, interaction_type, last_seen_at) VALUES (:d,'week',:a,:b,1,'friend',:ts)"),
            {"d": today, "a": a, "b": b, "ts": datetime.utcnow()},
        )
    return pd.DataFrame([{"user_id": k, "pagerank": v} for k, v in pr.items()])


def _build_company_health(db: Session) -> None:
    db.execute(text("DELETE FROM mart_company_health_period"))
    db.execute(
        text(
            """
            INSERT INTO mart_company_health_period
            SELECT
              current_date, 'week', u.company_id,
              COALESCE(100.0 * (COUNT(DISTINCT CASE WHEN COALESCE(s.logins,0) > 0 THEN u.user_id END)::DOUBLE / NULLIF(COUNT(DISTINCT u.user_id),0)),0) as score,
              COUNT(DISTINCT CASE WHEN COALESCE(s.logins,0) > 0 THEN u.user_id END) as active_users,
              COUNT(DISTINCT CASE WHEN (COALESCE(s.logins,0)+COALESCE(s.downloads,0)+COALESCE(s.discussion_replies,0)+COALESCE(s.threads_created,0)+COALESCE(s.blogs_created,0)) > 0 THEN u.user_id END) engaged_users,
              COUNT(DISTINCT CASE WHEN (COALESCE(s.threads_created,0)+COALESCE(s.blogs_created,0)) > 0 THEN u.user_id END) contributors,
              COALESCE(SUM(s.threads_created),0), COALESCE(SUM(s.discussion_replies),0), COALESCE(SUM(s.downloads),0),
              0, '[]'
            FROM dim_user u
            LEFT JOIN fact_user_activity_snapshot s ON s.user_id = u.user_id
            GROUP BY 3
            """
        )
    )


def _build_topics(db: Session, cfg: dict) -> None:
    threads = pd.read_sql(text("SELECT thread_id, subject, COALESCE(total_replies,0) total_replies, COALESCE(created_at,current_timestamp) created_at FROM dim_thread WHERE subject IS NOT NULL"), db.bind)
    db.execute(text("DELETE FROM mart_topic_catalog"))
    db.execute(text("DELETE FROM mart_thread_topics"))
    db.execute(text("DELETE FROM mart_topic_metrics_period"))
    if threads.empty:
        return
    vec = TfidfVectorizer(stop_words="english", min_df=1)
    X = vec.fit_transform(threads["subject"].astype(str))
    n_clusters = min(max(2, cfg["topic_model"]["n_clusters_fallback"]), len(threads))
    model = KMeans(n_clusters=n_clusters, random_state=42, n_init="auto")
    labels = model.fit_predict(X)
    terms = np.array(vec.get_feature_names_out())
    threads["topic_id"] = labels.astype(str)
    for topic in sorted(set(labels)):
        center = model.cluster_centers_[topic]
        top = terms[np.argsort(center)[-5:][::-1]].tolist()
        db.execute(
            text("INSERT INTO mart_topic_catalog (topic_id, topic_label, top_keywords_json, model_version, created_at) VALUES (:id,:l,:k,'tfidf_kmeans_v1',:t)"),
            {"id": str(topic), "l": f"Topic {topic}", "k": json.dumps(top), "t": datetime.utcnow()},
        )
    for _, r in threads.iterrows():
        db.execute(text("INSERT INTO mart_thread_topics (thread_id, topic_id, topic_confidence) VALUES (:th,:tp,:c)"), {"th": r["thread_id"], "tp": r["topic_id"], "c": 0.7})

    topic_metrics = (
        threads.groupby("topic_id")
        .agg(threads=("thread_id", "count"), replies=("total_replies", "sum"))
        .reset_index()
    )
    for _, r in topic_metrics.iterrows():
        avg = r["replies"] / max(r["threads"], 1)
        db.execute(
            text(
                "INSERT INTO mart_topic_metrics_period (period_start_date, grain, topic_id, threads, replies, unique_participants, avg_replies_per_thread, company_diversity_index, new_engager_rate, influence_score, lift_vs_baseline, best_lag, lag_corr) VALUES (current_date,'week',:id,:th,:rp,:up,:avg,0,0,:i,0,0,0)"
            ),
            {"id": r["topic_id"], "th": int(r["threads"]), "rp": int(r["replies"]), "up": int(r["threads"]), "avg": float(avg), "i": float(avg)},
        )
