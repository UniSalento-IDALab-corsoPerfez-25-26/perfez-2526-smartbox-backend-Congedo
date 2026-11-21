"""Simulatore di flotta: invia al backend eventi e heartbeat realistici di più smartbox via HTTP.
Serve a provare dashboard, notifiche e statistiche senza camera né Raspberry.

Uso:  python tools/simulate_fleet.py --url http://localhost:8000/api/v1 --devices devices.yaml
      python tools/simulate_fleet.py ... --storico 7     (genera anche 7 giorni di eventi passati)
"""
import argparse, json, random, time, urllib.request, uuid
from datetime import datetime, timedelta, timezone
import yaml

REASONS = {1: ["PERCLOS", "sbadigli ripetuti", "volto assente"], 2: ["microsonno", "chinamento", "PERCLOS alto"]}


def post(url, token, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"})
    try:
        return urllib.request.urlopen(req, timeout=5).status
    except Exception as e:
        print("errore:", e); return None


def event(vehicle, level, prev, ts):
    reason = "rientro" if level < prev else random.choice(REASONS[level])
    return {"schema": "smartbox-evento/2", "event_id": str(uuid.uuid4()), "vehicle": vehicle,
            "ts": ts.astimezone().isoformat(timespec="milliseconds"), "level": level, "prev_level": prev,
            "reason": reason, "indicators": {"perclos": round(random.uniform(0.05, 0.4), 3),
                                             "ear": round(random.uniform(0.1, 0.3), 3)}, "sw": "4.0.0-sim"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000/api/v1")
    ap.add_argument("--devices", default="devices.yaml")
    ap.add_argument("--storico", type=int, default=0, help="giorni di storico da generare")
    a = ap.parse_args()
    devs = yaml.safe_load(open(a.devices, encoding="utf-8"))

    if a.storico:
        now = datetime.now(timezone.utc)
        for v, tok in devs.items():
            for _ in range(random.randint(8, 25) * a.storico // 7 + 1):
                # più episodi nelle ore notturne e primo pomeriggio, come in letteratura
                day = now - timedelta(days=random.uniform(0, a.storico))
                hour = random.choice([2, 3, 4, 5, 14, 15, 16, 22, 23] + list(range(6, 22)))
                ts = day.replace(hour=hour % 24, minute=random.randint(0, 59))
                lv = random.choices([1, 2], [0.65, 0.35])[0]
                post(f"{a.url}/events", tok, event(v, lv, 0, ts))
                post(f"{a.url}/events", tok, event(v, 0, lv, ts + timedelta(seconds=random.randint(6, 40))))
        print("storico generato")

    level = {v: 0 for v in devs}
    print("simulazione in tempo reale (Ctrl+C per fermare)")
    while True:
        for v, tok in devs.items():
            new = level[v]
            r = random.random()
            if level[v] == 0 and r < 0.06:
                new = random.choices([1, 2], [0.7, 0.3])[0]
            elif level[v] > 0 and r < 0.3:
                new = level[v] - 1
            if new != level[v]:
                post(f"{a.url}/events", tok, event(v, new, level[v], datetime.now(timezone.utc)))
                print(f"{v}: {level[v]} -> {new}")
                level[v] = new
            post(f"{a.url}/status", tok, {"schema": "smartbox-stato/1", "vehicle": v, "online": True,
                                          "level": level[v], "mode": "completa", "fps": round(random.uniform(13, 16), 1),
                                          "cpu_temp": round(random.uniform(52, 71), 1), "alert": "ok",
                                          "face_ratio": round(random.uniform(0.9, 1.0), 2),
                                          "perclos": round(random.uniform(0.02, 0.2), 3), "outbox_pending": 0,
                                          "sw": "4.0.0-sim",
                                          "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")})
        time.sleep(5)


if __name__ == "__main__":
    main()
