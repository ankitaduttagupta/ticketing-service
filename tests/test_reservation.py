import asyncio
import pytest
import httpx
import random

BASE = "http://localhost:8000"

@pytest.fixture
def ball_type():
    """
    Generate a unique ball_type for each test run.
    Prevents cross-test interference.
    """
    return str(random.randint(1000, 9999))


@pytest.mark.asyncio
async def test_preload_and_single_purchase(ball_type):
    """Preload tickets, purchase a single one, and validate counts"""
    tickets = [{"ticket_id": str(i), "numbers": [1,2,3]} for i in range(1, 11)]
    async with httpx.AsyncClient(timeout=10) as ac:
        # preload 10 tickets
        resp = await ac.post(f"{BASE}/preload/{ball_type}", json=tickets)
        assert resp.status_code == 200

        # purchase 1 ticket
        payload = {"player_id": "p1", "count": 1}
        resp2 = await ac.post(f"{BASE}/purchase/{ball_type}", json=payload)
        assert resp2.status_code == 200
        data = resp2.json()
        assert data["status"] == "purchased"
        assert len(data["tickets"]) == 1

        # check counts → 9 left, 1 sold
        resp3 = await ac.get(f"{BASE}/counts/{ball_type}")
        counts = resp3.json()
        assert counts["available"] == 9
        assert counts["sold"] == 1


@pytest.mark.asyncio
async def test_preload_and_batch_purchase(ball_type):
    """Preload 20 tickets, purchase 5 at once, verify counts"""
    tickets = [{"ticket_id": str(i), "numbers": [1,2,3]} for i in range(20, 40)]
    async with httpx.AsyncClient(timeout=10) as ac:
        resp = await ac.post(f"{BASE}/preload/{ball_type}", json=tickets)
        assert resp.status_code == 200

        payload = {"player_id": "p2", "count": 5}
        resp2 = await ac.post(f"{BASE}/purchase/{ball_type}", json=payload)
        assert resp2.status_code == 200
        data = resp2.json()
        assert data["status"] == "purchased"
        assert len(data["tickets"]) == 5

        resp3 = await ac.get(f"{BASE}/counts/{ball_type}")
        counts = resp3.json()
        assert counts["available"] == 15
        assert counts["sold"] == 5


@pytest.mark.asyncio
async def test_concurrent_purchases(ball_type):
    """20 players each buy 5 tickets concurrently (100 total)"""
    tickets = [{"ticket_id": str(i), "numbers": [1,2,3]} for i in range(100, 200)]
    async with httpx.AsyncClient(timeout=20) as ac:
        # preload 100 tickets
        resp = await ac.post(f"{BASE}/preload/{ball_type}", json=tickets)
        assert resp.status_code == 200

        async def buy(pid):
            payload = {"player_id": pid, "count": 5}
            return await ac.post(f"{BASE}/purchase/{ball_type}", json=payload)

        # 20 concurrent buyers
        tasks = [buy(f"p{i}") for i in range(20)]
        results = await asyncio.gather(*tasks)

        # all should succeed
        assert all(r.status_code == 200 for r in results)

        resp2 = await ac.get(f"{BASE}/counts/{ball_type}")
        counts = resp2.json()
        assert counts["available"] == 0
        assert counts["sold"] == 100


@pytest.mark.asyncio
async def test_over_purchase_not_allowed(ball_type):
    """Try buying more tickets than available"""
    tickets = [{"ticket_id": str(i), "numbers": [1,2,3]} for i in range(300, 305)]
    async with httpx.AsyncClient(timeout=10) as ac:
        resp = await ac.post(f"{BASE}/preload/{ball_type}", json=tickets)
        assert resp.status_code == 200

        # only 5 tickets loaded, but request 10
        payload = {"player_id": "pX", "count": 10}
        resp2 = await ac.post(f"{BASE}/purchase/{ball_type}", json=payload)

        # should reject (service must not oversell)
        assert resp2.status_code in (400, 409)
