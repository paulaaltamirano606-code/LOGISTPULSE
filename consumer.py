"""Streaming Data -> (1) ODS en tiempo real, (2) Data Lake raw, (3) Alertas / Monitoreo en tiempo real."""
import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer
from psycopg2.extras import execute_values

from .alerts import evaluate
from .common import RAW, pg, put_bytes

TOPIC = os.getenv("TELEMETRY_TOPIC", "amr.telemetry")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
DB_FLUSH_S = 2
LAKE_FLUSH_S = 30
ALERT_DEBOUNCE_S = 60
RETENTION = "2 hours"


def main():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "lp-consumer",
                         "auto.offset.reset": "latest", "enable.auto.commit": True})
    consumer.subscribe([TOPIC])
    conn = pg()
    conn.autocommit = True
    cur = conn.cursor()
    db_buf, lake_buf, last_alert = [], [], {}
    t_db = t_lake = t_ret = time.time()
    print(f"[consumer] {BOOTSTRAP}/{TOPIC} -> ods.amr_telemetry + lake-raw/telemetry + ods.alerts", flush=True)

    while True:
        msg = consumer.poll(1.0)
        now = time.time()
        if msg is not None and not msg.error():
            rec = json.loads(msg.value())
            db_buf.append((rec["ts"], rec["vehicle_id"], rec["x"], rec["y"], rec["speed_kmh"],
                           rec["battery_pct"], rec["temp_c"], rec["status"]))
            lake_buf.append(rec)
            for alert_type, detail in evaluate(rec):
                key = (rec["vehicle_id"], alert_type)
                if now - last_alert.get(key, 0) >= ALERT_DEBOUNCE_S:
                    last_alert[key] = now
                    cur.execute("INSERT INTO ods.alerts (ts, vehicle_id, alert_type, detail) VALUES (%s,%s,%s,%s)",
                                (rec["ts"], rec["vehicle_id"], alert_type, detail))
        if db_buf and now - t_db >= DB_FLUSH_S:
            execute_values(cur, "INSERT INTO ods.amr_telemetry VALUES %s", db_buf)
            db_buf, t_db = [], now
        if lake_buf and now - t_lake >= LAKE_FLUSH_S:
            ts = datetime.now(timezone.utc)
            key = f"telemetry/dt={ts:%Y-%m-%d}/hh={ts:%H}/part-{ts:%H%M%S}.jsonl"
            put_bytes(RAW, key, ("\n".join(json.dumps(r) for r in lake_buf) + "\n").encode())
            lake_buf, t_lake = [], now
        if now - t_ret >= 60:  # el ODS guarda solo lo reciente; el histórico vive en el Data Lake
            cur.execute(f"DELETE FROM ods.amr_telemetry WHERE ts < now() - interval '{RETENTION}'")
            t_ret = now


if __name__ == "__main__":
    main()
