"""Live PSK Reporter map backend: MQTT -> SQLite buffer -> WebSocket -> browser.

Run:  uvicorn server:app --host 127.0.0.1 --port 8000
Config via env vars (see below).
"""
import asyncio, json, os, time
from contextlib import asynccontextmanager

import aiomqtt, aiosqlite
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

HOST = os.getenv("MQTT_HOST", "mqtt.pskreporter.info")
PORT = int(os.getenv("MQTT_PORT", "1884"))                 # 1883 TCP, 1884 TLS
TOPIC = os.getenv("PSKR_TOPIC", "pskr/filter/v2/+/FT8/#")  # narrow this to cut load
TTL = int(os.getenv("TTL_MINUTES", "60")) * 60             # how long spots are kept
DB = os.getenv("DB_PATH", "spots.db")
MAX_PUSH = 300                                             # max spots per push per client

clients: dict[WebSocket, dict] = {}
queue: asyncio.Queue = asyncio.Queue(maxsize=100_000)
stats = {"received": 0, "connected": False}


def parse(payload: bytes):
    """Payload field names (t, b, md, f, rp, sc, sl, rc, rl) should be verified
    against a live message from mqtt.pskreporter.info."""
    try:
        d = json.loads(payload)
        if not (d.get("sl") and d.get("rl")):
            return None
        return {"t": int(d["t"]), "b": d.get("b"), "md": d.get("md"), "f": d.get("f"),
                "snr": d.get("rp"), "sc": d["sc"], "sl": d["sl"], "rc": d["rc"], "rl": d["rl"]}
    except (ValueError, KeyError, TypeError):
        return None


def matches(s: dict, f: dict) -> bool:
    if f.get("band") and s["b"] != f["band"]: return False
    if f.get("mode") and s["md"] != f["mode"]: return False
    c = (f.get("call") or "").upper()
    return not c or c in s["sc"].upper() or c in s["rc"].upper()


async def ingest():
    while True:
        try:
            async with aiomqtt.Client(HOST, PORT, tls_params=aiomqtt.TLSParameters() if PORT != 1883 else None) as c:
                await c.subscribe(TOPIC)
                stats["connected"] = True
                async for m in c.messages:
                    s = parse(m.payload)
                    if s and not queue.full():
                        queue.put_nowait(s)
                        stats["received"] += 1
        except aiomqtt.MqttError:
            stats["connected"] = False
            await asyncio.sleep(5)


async def writer():
    async with aiosqlite.connect(DB) as db:
        last_prune = 0
        while True:
            await asyncio.sleep(1)
            batch = []
            while not queue.empty() and len(batch) < 20_000:
                batch.append(queue.get_nowait())
            if batch:
                await db.executemany(
                    "INSERT INTO spots VALUES (:t,:b,:md,:f,:snr,:sc,:sl,:rc,:rl)", batch)
                await db.commit()
                for ws, flt in list(clients.items()):
                    out = [s for s in batch if matches(s, flt)][-MAX_PUSH:]
                    if out:
                        try: await ws.send_text(json.dumps(out))
                        except Exception: clients.pop(ws, None)
            if time.time() - last_prune > 60:
                await db.execute("DELETE FROM spots WHERE t < ?", (int(time.time()) - TTL,))
                await db.commit()
                last_prune = time.time()


@asynccontextmanager
async def lifespan(app):
    async with aiosqlite.connect(DB) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("CREATE TABLE IF NOT EXISTS spots(t INT,b TEXT,md TEXT,f INT,snr INT,"
                         "sc TEXT,sl TEXT,rc TEXT,rl TEXT)")
        await db.execute("CREATE INDEX IF NOT EXISTS i_t ON spots(t)")
        await db.commit()
    tasks = [asyncio.create_task(ingest()), asyncio.create_task(writer())]
    yield
    for t in tasks: t.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/api/spots")
async def spots(band: str = "", mode: str = "", call: str = "", minutes: int = 15, limit: int = 1500):
    q, args = "SELECT t,b,md,f,snr,sc,sl,rc,rl FROM spots WHERE t > ?", [int(time.time()) - min(minutes, TTL // 60) * 60]
    if band: q += " AND b=?"; args.append(band)
    if mode: q += " AND md=?"; args.append(mode)
    if call: q += " AND (sc LIKE ? OR rc LIKE ?)"; args += [f"%{call.upper()}%"] * 2
    q += " ORDER BY t DESC LIMIT ?"; args.append(min(limit, 5000))
    async with aiosqlite.connect(DB) as db:
        rows = await (await db.execute(q, args)).fetchall()
    keys = ("t", "b", "md", "f", "snr", "sc", "sl", "rc", "rl")
    return [dict(zip(keys, r)) for r in rows]


@app.get("/api/stats")
async def get_stats():
    return {**stats, "clients": len(clients), "topic": TOPIC}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients[ws] = {}
    try:
        while True:
            clients[ws] = json.loads(await ws.receive_text())  # client sends its filter
    except (WebSocketDisconnect, ValueError):
        clients.pop(ws, None)


@app.get("/")
async def index():
    return FileResponse("static/index.html")

app.mount("/static", StaticFiles(directory="static"), name="static")
