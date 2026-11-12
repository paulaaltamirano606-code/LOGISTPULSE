"""Siembra datos sintéticos de LOGISTPULSE en las fuentes del diagrama.

  Datos organizacionales -> Postgres (master.* y ods.*)
  Datos de terceros      -> Data Lake raw: third_party/carrier_sla.csv, third_party/weather_daily.jsonl
  Datos demográficos     -> Data Lake raw: demographics/zones.csv
  Redes sociales         -> Data Lake raw: social/reviews.jsonl
  Sensores / streaming   -> los genera `lp.producer` (Redpanda)
"""
import csv
import io
import json
import os
import random
from datetime import datetime, timedelta, timezone

from psycopg2.extras import execute_values

from .common import RAW, pg, put_bytes
from .sim import simulate

SEED = int(os.getenv("SEED", "42"))
N_ORDERS = int(os.getenv("N_ORDERS", "4000"))
DAYS = int(os.getenv("DAYS", "150"))
N_RECENT = int(os.getenv("N_RECENT", "120"))  # pedidos de las últimas horas (quedan en tránsito)

ZONES = [  # zone_id, name, city, population, avg_income_usd, vehicle_household_pct
    (1, "Norte (Carcelén)", "Quito", 310000, 780, 41),
    (2, "Sur (Quitumbe)", "Quito", 280000, 540, 22),
    (3, "Cumbayá", "Quito", 120000, 1650, 78),
    (4, "Valle de los Chillos", "Quito", 190000, 960, 55),
    (5, "Centro Histórico", "Quito", 60000, 610, 12),
    (6, "Norte (Alborada)", "Guayaquil", 350000, 820, 38),
    (7, "Sur (Guasmo)", "Guayaquil", 400000, 470, 15),
    (8, "Samborondón", "Guayaquil", 90000, 1900, 85),
    (9, "Centro", "Cuenca", 140000, 880, 36),
    (10, "Yanuncay", "Cuenca", 110000, 720, 33),
]
DEPOTS = [(1, "CD Quito Carcelén", "Quito"), (2, "CD Guayaquil Km 10", "Guayaquil"), (3, "CD Cuenca Parque Industrial", "Cuenca")]
CARRIERS = [(1, "Flota Propia"), (2, "TransAndes"), (3, "RapidoCosta"), (4, "Condor Express")]
CARRIER_SLA = [(1, 10, 0), (2, 12, 15), (3, 14, 25), (4, 12, 20)]  # carrier_id, sla_hours, sla_penalty_usd
CATEGORIES = ["Abarrotes", "Bebidas", "Limpieza", "Electrónica", "Textil", "Farmacia", "Ferretería"]
SEGMENT_WEIGHTS = [("RETAIL", 0.55), ("PYME", 0.30), ("CORPORATE", 0.15)]
DEPOT_PREFIX = {1: "QUI", 2: "GYE", 3: "CUE"}

REVIEW_GOOD = ["Llegó a tiempo, todo en orden.", "Excelente servicio, el repartidor fue amable.", "Rápido y sin problemas."]
REVIEW_BAD = ["Llegó tarde, mala experiencia.", "Esperé horas, nadie avisó del retraso.", "Pedido atrasado otra vez, pésimo."]
REVIEW_MID = ["Cumplió, aunque podría mejorar la comunicación.", "Normal, sin novedades."]


def build_master(rng):
    customers = []
    for cid in range(1, 201):
        seg = rng.choices([s for s, _ in SEGMENT_WEIGHTS], weights=[w for _, w in SEGMENT_WEIGHTS])[0]
        zone = rng.choice(ZONES)[0]
        customers.append((cid, f"Cliente {cid:03d}", seg, zone))
    products = [(pid, f"Producto {pid:02d}", rng.choice(CATEGORIES), round(rng.uniform(0.2, 18), 2)) for pid in range(1, 31)]
    vehicles = []
    for depot_id, _, _ in DEPOTS:
        p = DEPOT_PREFIX[depot_id]
        for i in range(1, 4):
            vehicles.append((f"{p}-AMR-{i:02d}", "AMR", depot_id, None))
        vehicles.append((f"{p}-VAN-01", "VAN", depot_id, 1))
        vehicles.append((f"{p}-VAN-02", "VAN", depot_id, 2))
        vehicles.append((f"{p}-TRK-01", "TRUCK", depot_id, 3 if depot_id != 3 else 4))
        vehicles.append((f"{p}-TRK-02", "TRUCK", depot_id, 4))
    return customers, products, vehicles


def build_weather(rng, days):
    today = datetime.now(timezone.utc).date()
    rows = []
    for back in range(days + 2, -1, -1):
        day = today - timedelta(days=back)
        for city in ("Quito", "Guayaquil", "Cuenca"):
            rainy = rng.random() < (0.35 if city != "Guayaquil" else 0.25)
            rain = round(rng.expovariate(1 / 9), 1) if rainy else 0.0
            cond = "LLUVIA" if rain > 10 else "LLOVIZNA" if rain > 2 else "DESPEJADO"
            rows.append({"date": day.isoformat(), "city": city, "rain_mm": rain, "condition": cond})
    return rows


def main():
    rng = random.Random(SEED)
    now = datetime.now(timezone.utc)
    customers, products, vehicles = build_master(rng)
    weather = build_weather(rng, DAYS)
    rain_by = {(w["date"], w["city"]): w["rain_mm"] for w in weather}
    zone_city = {z[0]: z[2] for z in ZONES}
    depot_by_city = {d[2]: d[0] for d in DEPOTS}
    depot_city = {d[0]: d[2] for d in DEPOTS}
    road_vehicles = {d: [v for v in vehicles if v[2] == d and v[1] != "AMR"] for d, _, _ in DEPOTS}

    orders, items, shipments, reviews = [], [], [], []
    for n in range(N_ORDERS):
        oid = 100001 + n
        cust = rng.choice(customers)
        city = zone_city[cust[3]]
        depot_id = depot_by_city[city]
        recent = n >= N_ORDERS - N_RECENT
        created = now - timedelta(hours=rng.uniform(0.3, 4.5)) if recent else now - timedelta(days=rng.uniform(0.6, DAYS))
        veh = rng.choice(road_vehicles[depot_id])
        distance = round(rng.uniform(4, 28) if city == "Quito" else rng.uniform(5, 40), 2)
        rain = rain_by.get((created.date().isoformat(), city), 0.0)
        promised, dispatched, delivered = simulate(rng, created, cust[2], distance, rain, veh[3])

        if rng.random() < 0.015 and not recent:  # cancelado antes de despachar
            orders.append((oid, cust[0], depot_id, created, promised, "CANCELLED"))
            continue
        for pid in rng.sample(range(1, 31), rng.randint(1, 5)):
            items.append((oid, pid, rng.randint(1, 12), round(rng.uniform(1.5, 120), 2)))

        if dispatched > now:  # todavía no sale de bodega
            orders.append((oid, cust[0], depot_id, created, promised, "CREATED"))
            shipments.append((oid + 500000, oid, veh[0], None, None, distance, "PENDING"))
        elif delivered > now:  # va en camino
            orders.append((oid, cust[0], depot_id, created, promised, "IN_TRANSIT"))
            shipments.append((oid + 500000, oid, veh[0], dispatched, None, distance, "IN_TRANSIT"))
        elif rng.random() < 0.02:  # entrega fallida
            orders.append((oid, cust[0], depot_id, created, promised, "IN_TRANSIT"))
            shipments.append((oid + 500000, oid, veh[0], dispatched, None, distance, "FAILED"))
        else:
            orders.append((oid, cust[0], depot_id, created, promised, "DELIVERED"))
            shipments.append((oid + 500000, oid, veh[0], dispatched, delivered, distance, "DELIVERED"))
            if rng.random() < 0.18:  # reseña en redes sociales
                late = delivered > promised
                rating = rng.choice([1, 1, 2, 2, 3]) if late else rng.choice([3, 4, 4, 5, 5])
                text = rng.choice(REVIEW_BAD if rating <= 2 else REVIEW_MID if rating == 3 else REVIEW_GOOD)
                reviews.append({
                    "review_id": len(reviews) + 1, "order_id": oid,
                    "platform": rng.choice(["facebook", "x", "google_maps", "pinterest"]),
                    "ts": (delivered + timedelta(hours=rng.uniform(0.2, 30))).isoformat(),
                    "rating": rating, "text": text,
                })

    # ---------- Postgres (master + ODS) ----------
    with pg() as conn, conn.cursor() as cur:
        cur.execute("""
            TRUNCATE dw.fact_shipment, dw.ml_delay_scores, dw.dim_vehicle, dw.dim_customer, dw.dim_carrier,
                     dw.dim_depot, dw.dim_zone, dw.dim_weather, ods.shipments, ods.order_items, ods.orders,
                     ods.amr_telemetry, ods.alerts, staging.zone_demo, staging.carrier_sla, staging.weather,
                     master.vehicle, master.customer, master.product, master.carrier, master.depot, master.zone
            RESTART IDENTITY CASCADE""")
        execute_values(cur, "INSERT INTO master.zone VALUES %s", [(z[0], z[1], z[2]) for z in ZONES])
        execute_values(cur, "INSERT INTO master.depot VALUES %s", DEPOTS)
        execute_values(cur, "INSERT INTO master.carrier VALUES %s", CARRIERS)
        execute_values(cur, "INSERT INTO master.vehicle VALUES %s", vehicles)
        execute_values(cur, "INSERT INTO master.customer VALUES %s", customers)
        execute_values(cur, "INSERT INTO master.product VALUES %s", products)
        execute_values(cur, "INSERT INTO ods.orders VALUES %s", orders)
        execute_values(cur, "INSERT INTO ods.order_items VALUES %s", items)
        execute_values(cur, "INSERT INTO ods.shipments VALUES %s", shipments)

    # ---------- Data Lake (zona raw) ----------
    def csv_bytes(header, rows):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(header)
        w.writerows(rows)
        return buf.getvalue().encode()

    def jsonl_bytes(rows):
        return ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n").encode()

    put_bytes(RAW, "demographics/zones.csv",
              csv_bytes(["zone_id", "population", "avg_income_usd", "vehicle_household_pct"], [(z[0], z[3], z[4], z[5]) for z in ZONES]))
    put_bytes(RAW, "third_party/carrier_sla.csv", csv_bytes(["carrier_id", "sla_hours", "sla_penalty_usd"], CARRIER_SLA))
    put_bytes(RAW, "third_party/weather_daily.jsonl", jsonl_bytes(weather))
    put_bytes(RAW, "social/reviews.jsonl", jsonl_bytes(reviews))

    n_late = sum(1 for s in shipments if s[6] == "DELIVERED" and s[3] and s[4])
    print(f"[seed] pedidos={len(orders)} despachos={len(shipments)} (entregados={n_late}) "
          f"items={len(items)} reseñas={len(reviews)} clima={len(weather)} vehículos={len(vehicles)}")


if __name__ == "__main__":
    main()
