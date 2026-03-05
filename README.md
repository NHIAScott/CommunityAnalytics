# NHIA Community Analytics

Production-ready analytics web app for NHIA online community engagement reporting.

## Stack
- Backend: FastAPI + DuckDB + Pandas + NetworkX
- Frontend: Next.js (App Router) + TypeScript
- Storage: DuckDB file persisted via Docker volume

## Features Implemented
- Multi-file Excel ingestion with drag/drop UI.
- Dataset detection by column signatures and column normalization.
- File dedupe via SHA-256 hash and ingestion log.
- Canonical schema for users, companies, activity snapshots, discussions, friend requests.
- Materialized marts for user scores, company health, topics, and network metrics.
- Explainable scoring with configurable weights in `backend/config/scoring.yaml`.
- Topic clustering pipeline (TF-IDF + KMeans fallback; BERTopic extension point).
- Network centrality pipeline (PageRank, betweenness, reciprocity).
- Dashboard pages: ingestion, executive overview, users, companies, topics, network, metrics dictionary.
- CSV-style export endpoint (`/api/exports`).

## Run with Docker
```bash
docker compose up --build
```
Backend: http://localhost:8000
Frontend: http://localhost:3000

## Local Dev
Backend:
```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```
Frontend:
```bash
cd frontend
npm install
npm run dev
```

## Uploading Exports
1. Open `/ingestion` page.
2. Select one or multiple Excel exports.
3. App stores raw files and ingestion metadata, then triggers materialization jobs.

## Scoring Weights
Adjust `backend/config/scoring.yaml`:
- Engagement weight formula
- Super-user blend
- Company health components
- Rolling window days
- Topic model config

## Explainability
- `mart_user_scores_period.drivers_json` stores top score contributors.
- `mart_company_health_period.risk_flags_json` reserved for explainable early warnings.
- `run_metadata` stores scoring config and run context per materialization.

## Data Hygiene
- User ID precedence: integration_id > contact_key > email > name+company hash.
- Company canonicalization + alias mapping table (`company_alias_map`).
- Duplicate upload handling by content hash.
- Partial-ingestion tolerant: unknown sheets are logged as warnings.

## API Endpoints
- `GET/POST /api/ingestions`
- `GET /api/overview`
- `GET /api/users`
- `GET /api/companies`
- `GET /api/topics`
- `GET /api/network`
- `GET /api/exports`

All list endpoints support extension for global filters (`date_start`, `date_end`, `grain`, `community`, `company_id`, `state`, `country`, `tier`, `topic_id`).

## Notes
- Timezone for UI/business semantics: America/New_York.
- Optional external APIs are disabled by default.
- Designed so DuckDB can be swapped for Postgres with SQLAlchemy-compatible repository layer.
