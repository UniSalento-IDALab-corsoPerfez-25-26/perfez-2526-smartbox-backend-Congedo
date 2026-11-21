"""Backend smartbox: gateway e dashboard di flotta (Componenti 2 e 4 della proposta IDA).

Riceve dalle smartbox SOLO sintesi di eventi e stati (mai immagini), per due vie:
  - HTTP:  POST /api/v1/events, POST /api/v1/status   (token Bearer per dispositivo, devices.yaml)
  - MQTT:  sottoscrizione a smartbox/+/eventi e smartbox/+/stato (sessione persistente, QoS 1)
Deduplica gli eventi per event_id (consegna "almeno una volta" -> archivio "esattamente una volta"),
li conserva in SQLite per RETENTION_DAYS, li spinge in tempo reale alla dashboard via WebSocket
e, se configurato, invia una notifica push ntfy sulle salite di livello.

Avvio:  uvicorn app:app --host 0.0.0.0 --port 8000      (dalla cartella del repository)
"""
import asyncio, json, os, sqlite3, threading, time, urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

HERE = Path(__file__).resolve().parent
DB_PATH = os.getenv("SMARTBOX_DB", str(HERE / "smartbox.sqlite"))
DEVICES_PATH = os.getenv("SMARTBOX_DEVICES", str(HERE / "devices.yaml"))
DASHBOARD_TOKEN = os.getenv("SMARTBOX_DASHBOARD_TOKEN", "")      # vuoto = dashboard aperta (solo laboratorio)
RETENTION_DAYS = int(os.getenv("SMARTBOX_RETENTION_DAYS", "30"))
OFFLINE_AFTER = int(os.getenv("SMARTBOX_OFFLINE_AFTER", "35"))    # s senza heartbeat -> offline
MQTT_HOST = os.getenv("SMARTBOX_MQTT_HOST", "")                    # vuoto = bridge MQTT disattivato
MQTT_PORT = int(os.getenv("SMARTBOX_MQTT_PORT", "1883"))
MQTT_USER = os.getenv("SMARTBOX_MQTT_USER", "")
MQTT_PASS = os.getenv("SMARTBOX_MQTT_PASS", "")
MQTT_TLS = os.getenv("SMARTBOX_MQTT_TLS", "0") == "1"
NTFY_SERVER = os.getenv("SMARTBOX_NTFY_SERVER", "https://ntfy.sh")
NTFY_TOPIC = os.getenv("SMARTBOX_NTFY_TOPIC", "")                  # vuoto = niente push
NTFY_TOKEN = os.getenv("SMARTBOX_NTFY_TOKEN", "")
REASONS_LABEL = {2: "PERICOLO", 1: "ATTENZIONE", 0: "normale"}


# ---------------------------------------------------------------- modelli dei messaggi
class EventIn(BaseModel):
    schema_: str = Field(alias="schema")
    event_id: str = Field(min_length=8, max_length=64)
    vehicle: str = Field(max_length=64)
    ts: str
    level: int = Field(ge=0, le=2)
    prev_level: int = Field(0, ge=0, le=2)
    reason: str = Field(max_length=64)
    indicators: dict = {}
    sw: Optional[str] = None


class StatusIn(BaseModel):
    schema_: str = Field("smartbox-stato/1", alias="schema")
    vehicle: str = Field(max_length=64)
    ts: Optional[str] = None
    online: bool = True
    level: Optional[int] = None
    mode: Optional[str] = None
    fps: Optional[float] = None
    cpu_temp: Optional[float] = None
    alert: Optional[str] = None
    face_ratio: Optional[float] = None
    perclos: Optional[float] = None
    outbox_pending: Optional[int] = None
    sw: Optional[str] = None


# ---------------------------------------------------------------- archivio
class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock:
            self.db.executescript("""
            CREATE TABLE IF NOT EXISTS events(
              event_id TEXT PRIMARY KEY, vehicle TEXT NOT NULL, ts TEXT NOT NULL, received_at TEXT NOT NULL,
              level INTEGER NOT NULL, prev_level INTEGER, reason TEXT, perclos REAL, ear REAL, via TEXT, sw TEXT);
            CREATE INDEX IF NOT EXISTS ix_events_vehicle_ts ON events(vehicle, ts);
            CREATE TABLE IF NOT EXISTS status(
              vehicle TEXT PRIMARY KEY, ts TEXT, received_at TEXT, received_epoch REAL, online INTEGER,
              level INTEGER, mode TEXT, fps REAL, cpu_temp REAL, alert TEXT, face_ratio REAL,
              perclos REAL, outbox_pending INTEGER, sw TEXT);
            """)

    def add_event(self, e: EventIn, via: str) -> bool:
        """True se nuovo, False se duplicato (stesso event_id già ricevuto)."""
        ind = e.indicators or {}
        with self.lock:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (e.event_id, e.vehicle, e.ts, utcnow(), e.level, e.prev_level, e.reason,
                 ind.get("perclos"), ind.get("ear"), via, e.sw))
            self.db.commit()
            return cur.rowcount == 1

    def set_status(self, s: StatusIn):
        d = s.model_dump(by_alias=False, exclude={"schema_"})
        with self.lock:
            self.db.execute("""INSERT INTO status(vehicle, ts, received_at, received_epoch, online, level, mode, fps,
                cpu_temp, alert, face_ratio, perclos, outbox_pending, sw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(vehicle) DO UPDATE SET ts=excluded.ts, received_at=excluded.received_at,
                received_epoch=excluded.received_epoch, online=excluded.online,
                level=COALESCE(excluded.level, status.level), mode=COALESCE(excluded.mode, status.mode),
                fps=COALESCE(excluded.fps, status.fps), cpu_temp=COALESCE(excluded.cpu_temp, status.cpu_temp),
                alert=COALESCE(excluded.alert, status.alert), face_ratio=COALESCE(excluded.face_ratio, status.face_ratio),
                perclos=COALESCE(excluded.perclos, status.perclos),
                outbox_pending=COALESCE(excluded.outbox_pending, status.outbox_pending), sw=COALESCE(excluded.sw, status.sw)""",
                (d["vehicle"], d["ts"], utcnow(), time.time(), int(d["online"]), d["level"], d["mode"], d["fps"],
                 d["cpu_temp"], d["alert"], d["face_ratio"], d["perclos"], d["outbox_pending"], d["sw"]))
            self.db.commit()

    def query(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.db.execute(sql, args).fetchall()]

    def purge(self):
        limit = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
        with self.lock:
            n = self.db.execute("DELETE FROM events WHERE received_at < ?", (limit,)).rowcount
            self.db.commit()
        return n


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def load_devices():
    """devices.yaml:  veicolo-01: token-segreto   (un token per dispositivo)"""
    if not os.path.exists(DEVICES_PATH):
        return {}
    return {str(k): str(v) for k, v in (yaml.safe_load(open(DEVICES_PATH, encoding="utf-8")) or {}).items()}


store = Store(DB_PATH)
DEVICES = load_devices()


# ---------------------------------------------------------------- WebSocket: push in tempo reale
class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    async def broadcast(self, msg: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)

    def broadcast_threadsafe(self, msg: dict):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.broadcast(msg), self.loop)


hub = Hub()


# ---------------------------------------------------------------- ingresso comune HTTP/MQTT
def notify_ntfy(e: EventIn):
    if not NTFY_TOPIC or e.level <= e.prev_level or e.level == 0:
        return
    req = urllib.request.Request(
        f"{NTFY_SERVER.rstrip('/')}/{NTFY_TOPIC}", method="POST",
        data=f"{e.vehicle}: {e.reason} (PERCLOS {e.indicators.get('perclos', '-')})".encode(),
        headers={"Title": f"Smartbox – {REASONS_LABEL[e.level]}", "Priority": "urgent" if e.level == 2 else "high",
                 "Tags": "rotating_light" if e.level == 2 else "warning",
                 **({"Authorization": f"Bearer {NTFY_TOKEN}"} if NTFY_TOKEN else {})})
    try:
        urllib.request.urlopen(req, timeout=5).read()
    except Exception as ex:
        print(f"[ntfy] invio fallito: {ex}")


def ingest_event(e: EventIn, via: str) -> bool:
    new = store.add_event(e, via)
    if new:
        hub.broadcast_threadsafe({"type": "event", "data": {**e.model_dump(by_alias=True), "via": via}})
        threading.Thread(target=notify_ntfy, args=(e,), daemon=True).start()
    return new


def ingest_status(s: StatusIn):
    store.set_status(s)
    hub.broadcast_threadsafe({"type": "status", "data": s.model_dump(by_alias=True)})


# ---------------------------------------------------------------- bridge MQTT (topologia A della tesi)
def start_mqtt_bridge():
    if not MQTT_HOST:
        return None
    import paho.mqtt.client as mqtt
    try:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="smartbox-backend", clean_session=False)
    except AttributeError:
        c = mqtt.Client(client_id="smartbox-backend", clean_session=False)
    if MQTT_USER:
        c.username_pw_set(MQTT_USER, MQTT_PASS)
    if MQTT_TLS:
        c.tls_set()

    def on_connect(client, *a):
        # sessione persistente + QoS 1: il broker conserva gli eventi arrivati mentre il backend era giù
        client.subscribe([("smartbox/+/eventi", 1), ("smartbox/+/stato", 1)])
        print("[mqtt] bridge connesso")

    def on_message(client, userdata, msg):
        try:
            body = json.loads(msg.payload)
            vehicle_topic = msg.topic.split("/")[1]
            if body.get("vehicle") != vehicle_topic:      # un dispositivo pubblica solo per sé (con ACL sul broker)
                return
            if msg.topic.endswith("/eventi"):
                ingest_event(EventIn(**body), "mqtt")
            else:
                ingest_status(StatusIn(**body))
        except Exception as ex:
            print(f"[mqtt] messaggio scartato su {msg.topic}: {ex}")

    c.on_connect, c.on_message = on_connect, on_message
    c.reconnect_delay_set(1, 30)
    c.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    c.loop_start()
    return c


async def housekeeping():
    while True:
        n = await asyncio.to_thread(store.purge)
        if n:
            print(f"[retention] cancellati {n} eventi più vecchi di {RETENTION_DAYS} giorni")
        await asyncio.sleep(3600)


@asynccontextmanager
async def lifespan(app):
    hub.loop = asyncio.get_running_loop()
    bridge = start_mqtt_bridge()
    task = asyncio.create_task(housekeeping())
    yield
    task.cancel()
    if bridge:
        bridge.loop_stop(); bridge.disconnect()


app = FastAPI(title="Smartbox backend", version="4.0.0", lifespan=lifespan)


# ---------------------------------------------------------------- autenticazione
def _bearer(authorization: Optional[str]):
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


def device_auth(vehicle: str, authorization: Optional[str]):
    tok = _bearer(authorization)
    if not DEVICES.get(vehicle) or tok != DEVICES[vehicle]:
        raise HTTPException(401, "token dispositivo non valido per questo veicolo")


def dashboard_auth(authorization: Optional[str] = Header(None)):
    if DASHBOARD_TOKEN and _bearer(authorization) != DASHBOARD_TOKEN:
        raise HTTPException(401, "token dashboard richiesto")


# ---------------------------------------------------------------- API dispositivi
@app.post("/api/v1/events")
def post_event(e: EventIn, authorization: Optional[str] = Header(None)):
    device_auth(e.vehicle, authorization)
    return {"ok": True, "duplicate": not ingest_event(e, "http")}


@app.post("/api/v1/status")
def post_status(s: StatusIn, authorization: Optional[str] = Header(None)):
    device_auth(s.vehicle, authorization)
    ingest_status(s)
    return {"ok": True}


# ---------------------------------------------------------------- API dashboard
@app.get("/api/v1/config", dependencies=[Depends(dashboard_auth)])
def get_config():
    return {"retention_days": RETENTION_DAYS, "offline_after": OFFLINE_AFTER,
            "mqtt_bridge": bool(MQTT_HOST), "ntfy": bool(NTFY_TOPIC), "devices": sorted(DEVICES)}


@app.get("/api/v1/vehicles", dependencies=[Depends(dashboard_auth)])
def get_vehicles():
    rows = {r["vehicle"]: r for r in store.query("SELECT * FROM status")}
    last = store.query("""SELECT vehicle, MAX(ts) AS last_event_ts, COUNT(*) AS events_24h,
                          SUM(level=2 AND level>prev_level) AS danger_24h FROM events
                          WHERE received_at >= ? GROUP BY vehicle""",
                       ((datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),))
    out = []
    for v in sorted(set(rows) | set(DEVICES) | {r["vehicle"] for r in last}):
        s = rows.get(v, {"vehicle": v})
        ev = next((r for r in last if r["vehicle"] == v), {})
        age = time.time() - s["received_epoch"] if s.get("received_epoch") else None
        out.append({**s, "online": bool(s.get("online")) and age is not None and age < OFFLINE_AFTER,
                    "seconds_since_status": round(age) if age is not None else None,
                    "events_24h": ev.get("events_24h", 0), "danger_24h": ev.get("danger_24h") or 0,
                    "last_event_ts": ev.get("last_event_ts")})
    return out


@app.get("/api/v1/events", dependencies=[Depends(dashboard_auth)])
def get_events(vehicle: Optional[str] = None, min_level: int = Query(0, ge=0, le=2),
               since: Optional[str] = None, only_alarms: bool = False, limit: int = Query(200, le=5000)):
    sql, args = "SELECT * FROM events WHERE level >= ?", [min_level]
    if vehicle:
        sql += " AND vehicle = ?"; args.append(vehicle)
    if since:
        sql += " AND ts >= ?"; args.append(since)
    if only_alarms:
        sql += " AND level > prev_level"
    sql += " ORDER BY ts DESC LIMIT ?"; args.append(limit)
    return store.query(sql, args)


@app.get("/api/v1/stats", dependencies=[Depends(dashboard_auth)])
def get_stats(days: int = Query(7, ge=1, le=365), vehicle: Optional[str] = None):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    flt, args = "WHERE received_at >= ? AND level > prev_level", [since]
    if vehicle:
        flt += " AND vehicle = ?"; args.append(vehicle)
    return {
        "days": days,
        "by_day": store.query(f"""SELECT substr(ts,1,10) AS day, SUM(level=1) AS l1, SUM(level=2) AS l2
                                  FROM events {flt} GROUP BY day ORDER BY day""", args),
        "by_reason": store.query(f"""SELECT reason, COUNT(*) AS n FROM events {flt}
                                     GROUP BY reason ORDER BY n DESC""", args),
        "by_hour": store.query(f"""SELECT CAST(substr(ts,12,2) AS INTEGER) AS hour, COUNT(*) AS n
                                   FROM events {flt} GROUP BY hour ORDER BY hour""", args),
        "totals": store.query(f"""SELECT COUNT(*) AS alarms, SUM(level=2) AS danger, SUM(level=1) AS warning,
                                  COUNT(DISTINCT vehicle) AS vehicles FROM events {flt}""", args)[0],
    }


@app.get("/api/v1/export.csv", dependencies=[Depends(dashboard_auth)], response_class=PlainTextResponse)
def export_csv(vehicle: Optional[str] = None):
    rows = get_events(vehicle=vehicle, min_level=0, since=None, only_alarms=False, limit=5000)
    head = ["event_id", "vehicle", "ts", "level", "prev_level", "reason", "perclos", "ear", "via"]
    lines = [",".join(head)] + [",".join("" if r[k] is None else str(r[k]) for k in head) for r in rows]
    return PlainTextResponse("\n".join(lines), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=eventi_smartbox.csv"})


@app.websocket("/ws")
async def ws(websocket: WebSocket, token: str = ""):
    if DASHBOARD_TOKEN and token != DASHBOARD_TOKEN:
        await websocket.close(code=4401); return
    await websocket.accept()
    hub.clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()          # ping dal client; i dati vanno solo verso il client
    except WebSocketDisconnect:
        hub.clients.discard(websocket)


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": utcnow()}


# la dashboard è servita dallo stesso processo: niente CORS, un solo indirizzo da ricordare.
# È nel repository perfez-2526-smartbox-dashboard-Congedo: clonato accanto a questo lo si trova da solo,
# altrimenti si indica la sua cartella con SMARTBOX_FRONTEND_DIR. Senza dashboard restano attive le API.
FRONTEND_DIR = Path(os.getenv("SMARTBOX_FRONTEND_DIR", str(HERE.parent / "perfez-2526-smartbox-dashboard-Congedo")))
if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    print(f"[backend] dashboard non trovata in {FRONTEND_DIR}: attive solo le API (/docs)")
