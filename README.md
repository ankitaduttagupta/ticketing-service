# Ticket Reservation Service (Redis + FastAPI)

This repository contains a production-ready sketch of a ticket reservation system using Redis for high-concurrency reservation,
FastAPI for the API, and Lua scripts for atomic multi-reserve/confirm/rollback operations.

## Features
- Hash for immutable ticket details
- Sets for available/reserved/sold state
- Lua scripts for atomic reserve/confirm/rollback
- Reservation expiry ZSET + sweeper pattern
- Dockerized (app + redis) via docker-compose
- Basic integration tests (pytest) that assume services are running

## Quickstart (local)
1. Build and start services:
```bash
docker-compose up --build -d
```
2. Wait a few seconds for services to start, then run tests (locally):
```bash
pytest tests/test_reservation.py -q
```
3. Open API docs: http://localhost:8000/docs

## Notes
- Lua scripts are stored in `app/main.py` as string constants and registered on startup.
- For production, tune `lease_seconds`, Redis persistence settings, cluster, and connection pool sizes.
- For Redis Cluster, keys use a hash-tag `{bt}` so related keys are colocated. Ensure you run cluster with proper slot config.

