"""ETL en lotes:  Data Lake (raw) + ODS + Master  ->  Data Warehouse  ->  Data Lake (curated).

Flechas del diagrama que implementa:
  * Datos de terceros / demográficos  --(lote)-->  staging  --> dimensiones del DW
  * ODS + Master Data                 --(lote)-->  hechos y dimensiones del DW
  * DW  --(export parquet)-->  Data Lake curated (consumible por DuckDB/Spark en la capa "Hadoop")
"""
import csv
import io
import json
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from .common import CURATED, RAW, get_bytes, pg, put_bytes, query_df


def load_staging(cur):
    zones = list(csv.DictReader(io.StringIO(get_bytes(RAW, "demographics/zones.csv").decode())))
    sla = list(csv.DictReader(io.StringIO(get_bytes(RAW, "third_party/carrier_sla.csv").decode())))
    weather = [json.loads(line) for line in get_bytes(RAW, "third_party/weather_daily.jsonl").decode().splitlines() if line]

    cur.execute("TRUNCATE staging.zone_demo, staging.carrier_sla, staging.weather")
    execute_values(cur, "INSERT INTO staging.zone_demo VALUES %s",
                   [(int(z["zone_id"]), int(z["population"]), float(z["avg_income_usd"]), float(z["vehicle_household_pct"])) for z in zones])
    execute_values(cur, "INSERT INTO staging.carrier_sla VALUES %s",
                   [(int(s["carrier_id"]), float(s["sla_hours"]), float(s["sla_penalty_usd"])) for s in sla])
    execute_values(cur, "INSERT INTO staging.weather VALUES %s",
                   [(w["date"], w["city"], w["rain_mm"], w["condition"]) for w in weather])
    return len(zones), len(sla), len(weather)


def export_curated() -> int:
    df = query_df("SELECT * FROM semantic.v_shipments")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    put_bytes(CURATED, "shipments/shipments.parquet", buf.getvalue())
    return len(df)


def main():
    sql = (Path(__file__).parent / "etl_dw.sql").read_text()
    with pg() as conn, conn.cursor() as cur:
        z, s, w = load_staging(cur)
        cur.execute(sql)
        cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE status='DELIVERED'), COUNT(*) FILTER (WHERE status='IN_TRANSIT') FROM dw.fact_shipment")
        total, delivered, transit = cur.fetchone()
    n = export_curated()
    print(f"[etl] staging(zonas={z}, sla={s}, clima={w}) -> dw.fact_shipment={total} "
          f"(entregados={delivered}, en tránsito={transit}) -> parquet curated filas={n}")


if __name__ == "__main__":
    main()
