import asyncio
import json
import time
from typing import List, Optional, AsyncGenerator
from fastapi import FastAPI, HTTPException
import redis.asyncio as redis
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Lua scripts (as raw strings)
RESERVE_N_LUA = r"""
-- RESERVE_N_LUA
-- KEYS: 1=available, 2=reserved, 3=pool, 4=reserved_exp_zset
-- ARGV: 1=n, 2=expiry_ts

local n = tonumber(ARGV[1])
local expiry = tonumber(ARGV[2])
local out = {}
for i=1,n do
  local id = redis.call('SPOP', KEYS[1])
  if not id then
    break
  end
  redis.call('SADD', KEYS[2], id)
  redis.call('ZADD', KEYS[4], expiry, id)
  local ticket = redis.call('HGET', KEYS[3], id) or ""
  table.insert(out, id)
  table.insert(out, ticket)
end
return out
"""

CONFIRM_N_LUA = r"""
-- CONFIRM_N_LUA
-- KEYS: 1=reserved, 2=sold, 3=reserved_exp_zset
-- ARGV: list of ids...

for i=1,#ARGV do
  local id = ARGV[i]
  redis.call('SMOVE', KEYS[1], KEYS[2], id)
  redis.call('ZREM', KEYS[3], id)
end
return #ARGV
"""

ROLLBACK_N_LUA = r"""
-- ROLLBACK_N_LUA
-- KEYS: 1=reserved, 2=available, 3=reserved_exp_zset
-- ARGV: list of ids...

for i=1,#ARGV do
  local id = ARGV[i]
  redis.call('SMOVE', KEYS[1], KEYS[2], id)
  redis.call('ZREM', KEYS[3], id)
end
return #ARGV
"""

RECLAIM_EXPIRED_LUA = r"""
-- RECLAIM_EXPIRED_LUA
-- KEYS: 1=reserved, 2=available, 3=reserved_exp_zset
-- ARGV: 1=now_ts, 2=limit

local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local ids = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, limit)
if #ids == 0 then
  return {}
end
for i=1,#ids do
  local id = ids[i]
  redis.call('SMOVE', KEYS[1], KEYS[2], id)
  redis.call('ZREM', KEYS[3], id)
end
return ids
"""

# FastAPI app
# We'll set Redis URL via env var or default to docker compose service name
REDIS_URL = "redis://redis:6379/0"

# Globals to be set in lifespan
r: Optional[redis.Redis] = None
reserve_sha = None
confirm_sha = None
rollback_sha = None
reclaim_sha = None
sweeper_tasks = []

class PurchaseRequest(BaseModel):
    player_id: str
    count: int = 1

def key_names(bt: int):
    tag = f"{{{bt}}}"  # hash tag for redis cluster colocation
    return {
        "available": f"ball:{tag}:available",
        "reserved":  f"ball:{tag}:reserved",
        "sold":      f"ball:{tag}:sold",
        "pool":      f"ball:{tag}:pool",
        "reserved_exp": f"ball:{tag}:reserved:exp",
    }

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global r, reserve_sha, confirm_sha, rollback_sha, reclaim_sha, sweeper_tasks
    r = redis.from_url(REDIS_URL, decode_responses=True, max_connections=100)
    # Load Lua scripts
    reserve_sha = await r.script_load(RESERVE_N_LUA)
    confirm_sha = await r.script_load(CONFIRM_N_LUA)
    rollback_sha = await r.script_load(ROLLBACK_N_LUA)
    reclaim_sha = await r.script_load(RECLAIM_EXPIRED_LUA)
    # warm up
    await r.ping()

    # start background sweepers for example ball types (adjust as needed)
    for bt in [90, 75, 60, 45, 30]:
        task = asyncio.create_task(_sweeper_loop(bt, batch=500))
        sweeper_tasks.append(task)

    try:
        yield
    finally:
        # cancel sweepers
        for t in sweeper_tasks:
            t.cancel()
        await asyncio.gather(*sweeper_tasks, return_exceptions=True)
        # close redis
        if r:
            await r.close()

app = FastAPI(title="Ticket Reservation Service", lifespan=lifespan)

@app.post("/preload/{bt}")
async def preload(bt: int, tickets: List[dict]):
    """Preload tickets into Redis (pool + available set). Use in dev/test only."""
    k = key_names(bt)
    pipe = r.pipeline(transaction=False)
    for t in tickets:
        tid = str(t["ticket_id"])
        pipe.hset(k["pool"], tid, json.dumps(t))
        pipe.sadd(k["available"], tid)
    await pipe.execute()
    return {"loaded": len(tickets)}

async def reserve_n(bt: int, n: int, lease_seconds: int):
    k = key_names(bt)
    expiry_ts = int(time.time()) + lease_seconds
    # EVALSHA reserve script
    res = await r.evalsha(reserve_sha, 4, k["available"], k["reserved"], k["pool"], k["reserved_exp"], n, expiry_ts)
    pairs = []
    if not res:
        return pairs
    for i in range(0, len(res), 2):
        tid = res[i]
        ticket_json = res[i+1]
        pairs.append((tid, json.loads(ticket_json) if ticket_json else None))
    return pairs

async def confirm_ids(bt: int, ids: List[str]):
    k = key_names(bt)
    if not ids:
        return 0
    res = await r.evalsha(confirm_sha, 3, k["reserved"], k["sold"], k["reserved_exp"], *ids)
    return int(res or 0)

async def rollback_ids(bt: int, ids: List[str]):
    k = key_names(bt)
    if not ids:
        return 0
    res = await r.evalsha(rollback_sha, 3, k["reserved"], k["available"], k["reserved_exp"], *ids)
    return int(res or 0)

@app.post("/purchase/{bt}")
async def purchase(bt: int, req: PurchaseRequest):
    """Reserve N tickets for player; call wallet (simulated) and confirm/rollback"""
    lease_seconds = 30
    k = key_names(bt)

    # ✅ Pre-check available tickets count
    avail = await r.scard(k["available"])
    if avail < req.count:
        raise HTTPException(
            status_code=409,
            detail=f"Only {avail} tickets available, but {req.count} requested"
        )

    # 1) Reserve via Lua
    reserved = await reserve_n(bt, req.count, lease_seconds)
    if not reserved or len(reserved) < req.count:
        raise HTTPException(status_code=409, detail="Sold out or not enough tickets")

    ids = [tid for tid, _ in reserved]

    # 2) Mock wallet call
    wallet_ok = await call_wallet_debit(req.player_id, amount=len(ids)*10)
    if wallet_ok:
        moved = await confirm_ids(bt, ids)
        if moved != len(ids):
            # rollback on mismatch
            await rollback_ids(bt, ids)
            raise HTTPException(status_code=500, detail="Confirm mismatch; rolled back")
        return {"status": "purchased", "tickets": [t for _, t in reserved]}
    else:
        await rollback_ids(bt, ids)
        raise HTTPException(status_code=402, detail="Payment failed; rolled back")

# simple endpoint to check counts
@app.get("/counts/{bt}")
async def counts(bt: int):
    k = key_names(bt)
    avail = await r.scard(k["available"])
    reserved = await r.scard(k["reserved"])
    sold = await r.scard(k["sold"])
    return {"available": avail, "reserved": reserved, "sold": sold}

# simple mock wallet (replace with real integration)
async def call_wallet_debit(player_id: str, amount: int):
    await asyncio.sleep(0.01)
    # deterministic success for now; in tests you can mock this to fail
    return True

# Reclaimer endpoint (for manual / testing)
@app.post("/reclaim/{bt}")
async def reclaim(bt: int, limit: int = 1000):
    k = key_names(bt)
    now_ts = int(time.time())
    res = await r.evalsha(reclaim_sha, 3, k["reserved"], k["available"], k["reserved_exp"], now_ts, limit)
    return {"reclaimed": res}

async def _sweeper_loop(bt: int, batch: int = 500):
    k = key_names(bt)
    while True:
        try:
            now_ts = int(time.time())
            res = await r.evalsha(reclaim_sha, 3, k["reserved"], k["available"], k["reserved_exp"], now_ts, batch)
            if res:
                # optionally log reclaimed count
                pass
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(1)
            continue

# health
@app.get("/health")
async def health():
    try:
        pong = await r.ping()
        return {"redis": pong}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
