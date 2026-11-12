"""Sensores y dispositivos: simula telemetría de la flota (AMR, vans, camiones) hacia Redpanda (API Kafka).

Cada vehículo de master.vehicle emite un mensaje por tick. Se inyectan anomalías ocasionales
(batería baja, sobrecalentamiento, exceso de velocidad) para alimentar las alertas en tiempo real.
"""
import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .common import pg

TOPIC = os.getenv("TELEMETRY_TOPIC", "amr.telemetry")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TICK = float(os.getenv("TICK_SECONDS", "1"))
ANOMALY_PROB = float(os.getenv("ANOMALY_PROB", "0.004"))
SPEED_LIMIT = {"AMR": 5.0, "VAN": 80.0, "TRUCK": 70.0}


class Vehicle:
    def __init__(self, vehicle_id: str, kind: str, rng: random.Random):
        self.id, self.kind, self.rng = vehicle_id, kind, rng
        self.x, self.y = rng.uniform(0, 100), rng.uniform(0, 100)
        self.battery = rng.uniform(40, 100)
        self.temp = rng.uniform(22, 30)
        self.status = "MOVING"
        self.spike = 0  # ticks restantes de una anomalía térmica

    def step(self) -> dict:
        r, limit = self.rng, SPEED_LIMIT[self.kind]
        if self.status == "CHARGING":
            self.battery = min(100.0, self.battery + 1.8)
            speed = 0.0
            if self.battery >= 95:
                self.status = "MOVING"
        else:
            speed = max(0.0, r.gauss(limit * 0.55, limit * 0.12))
            if r.random() < ANOMALY_PROB:
                speed = limit * r.uniform(1.25, 1.6)  # exceso de velocidad
            self.x = (self.x + r.uniform(-1, 1) * (speed / 10 + 0.2)) % 100
            self.y = (self.y + r.uniform(-1, 1) * (speed / 10 + 0.2)) % 100
            self.battery = max(0.0, self.battery - (0.35 if self.kind == "AMR" else 0.08) * r.uniform(0.5, 1.5))
            if self.kind == "AMR" and self.battery < 8:
                self.status = "CHARGING"
            elif r.random() < ANOMALY_PROB and self.battery > 20:
                self.battery = r.uniform(9, 14)  # caída súbita
        if self.spike == 0 and r.random() < ANOMALY_PROB:
            self.spike = r.randint(8, 20)
        if self.spike > 0:
            self.temp += r.uniform(2, 4)
            self.spike -= 1
        else:
            self.temp += (26 - self.temp) * 0.2 + r.gauss(0, 0.4)
        return {
            "ts": datetime.now(timezone.utc).isoformat(), "vehicle_id": self.id,
            "x": round(self.x, 2), "y": round(self.y, 2), "speed_kmh": round(speed, 2),
            "battery_pct": round(self.battery, 1), "temp_c": round(self.temp, 1), "status": self.status,
        }


def ensure_topic(retries: int = 30):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for _ in range(retries):
        try:
            for future in admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)]).values():
                try:
                    future.result()
                except Exception as exc:  # el tópico ya existe
                    if "TOPIC_ALREADY_EXISTS" not in str(exc):
                        raise
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Redpanda no responde")


def main():
    rng = random.Random()
    with pg() as conn, conn.cursor() as cur:
        cur.execute("SELECT vehicle_id, kind FROM master.vehicle ORDER BY vehicle_id")
        fleet = [Vehicle(v, k, rng) for v, k in cur.fetchall()]
    if not fleet:
        raise SystemExit("[producer] master.vehicle vacío: corre `make seed` primero")
    ensure_topic()
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    print(f"[producer] {len(fleet)} vehículos -> {BOOTSTRAP}/{TOPIC} cada {TICK}s", flush=True)
    while True:
        started = time.time()
        for v in fleet:
            msg = v.step()
            producer.produce(TOPIC, key=v.id, value=json.dumps(msg).encode())
        producer.poll(0)
        time.sleep(max(0.0, TICK - (time.time() - started)))


if __name__ == "__main__":
    main()
