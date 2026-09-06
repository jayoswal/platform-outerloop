# Atlas Platform Outer Loop

Local orchestration, shared contracts, service templates, seed data, and
cross-service smoke tests for Atlas HRMS.

## Local setup

```bash
cd compose
cp .env.example .env
docker compose up --build -d
uv run --project .. python ../scripts/seed.py
uv run --project .. python ../scripts/smoke.py
```

The six repositories must be siblings beneath `/home/oswa/atlas-repos`.

