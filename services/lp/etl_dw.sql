-- ETL en lotes: ODS + Master + staging (Data Lake) -> Data Warehouse. Idempotente (upserts).

INSERT INTO dw.dim_zone (zone_id, name, city, population, avg_income_usd, vehicle_household_pct)
SELECT z.zone_id, z.name, z.city, d.population, d.avg_income_usd, d.vehicle_household_pct
FROM master.zone z
LEFT JOIN staging.zone_demo d ON d.zone_id = z.zone_id
ON CONFLICT (zone_id) DO UPDATE SET
    name = EXCLUDED.name, city = EXCLUDED.city, population = EXCLUDED.population,
    avg_income_usd = EXCLUDED.avg_income_usd, vehicle_household_pct = EXCLUDED.vehicle_household_pct;

INSERT INTO dw.dim_customer (customer_id, name, segment, zone_key)
SELECT c.customer_id, c.name, c.segment, z.zone_key
FROM master.customer c
JOIN dw.dim_zone z ON z.zone_id = c.zone_id
ON CONFLICT (customer_id) DO UPDATE SET
    name = EXCLUDED.name, segment = EXCLUDED.segment, zone_key = EXCLUDED.zone_key;

INSERT INTO dw.dim_depot (depot_id, name, city)
SELECT depot_id, name, city FROM master.depot
ON CONFLICT (depot_id) DO UPDATE SET name = EXCLUDED.name, city = EXCLUDED.city;

INSERT INTO dw.dim_carrier (carrier_id, name, sla_hours, sla_penalty_usd)
SELECT c.carrier_id, c.name, s.sla_hours, s.sla_penalty_usd
FROM master.carrier c
LEFT JOIN staging.carrier_sla s ON s.carrier_id = c.carrier_id
ON CONFLICT (carrier_id) DO UPDATE SET
    name = EXCLUDED.name, sla_hours = EXCLUDED.sla_hours, sla_penalty_usd = EXCLUDED.sla_penalty_usd;

INSERT INTO dw.dim_vehicle (vehicle_id, kind, depot_key, carrier_key)
SELECT v.vehicle_id, v.kind, d.depot_key, ca.carrier_key
FROM master.vehicle v
JOIN dw.dim_depot d ON d.depot_id = v.depot_id
LEFT JOIN dw.dim_carrier ca ON ca.carrier_id = v.carrier_id
ON CONFLICT (vehicle_id) DO UPDATE SET
    kind = EXCLUDED.kind, depot_key = EXCLUDED.depot_key, carrier_key = EXCLUDED.carrier_key;

INSERT INTO dw.dim_weather (date_key, city, rain_mm, condition)
SELECT to_char(date, 'YYYYMMDD')::int, city, rain_mm, condition FROM staging.weather
ON CONFLICT (date_key, city) DO UPDATE SET rain_mm = EXCLUDED.rain_mm, condition = EXCLUDED.condition;

INSERT INTO dw.fact_shipment (
    shipment_id, order_id, date_key, dispatch_hour, customer_key, depot_key, vehicle_key, carrier_key,
    order_value_usd, items, weight_kg, distance_km, lead_time_min, delay_min, on_time, status)
SELECT s.shipment_id,
       o.order_id,
       to_char(s.dispatched_at, 'YYYYMMDD')::int,
       EXTRACT(hour FROM s.dispatched_at)::smallint,
       dc.customer_key,
       dd.depot_key,
       dv.vehicle_key,
       dv.carrier_key,
       COALESCE(i.value, 0),
       COALESCE(i.items, 0),
       COALESCE(i.weight, 0),
       s.distance_km,
       ROUND((EXTRACT(epoch FROM (s.delivered_at - o.created_at)) / 60)::numeric, 1),
       ROUND((EXTRACT(epoch FROM (s.delivered_at - o.promised_at)) / 60)::numeric, 1),
       CASE WHEN s.delivered_at IS NULL THEN NULL ELSE s.delivered_at <= o.promised_at END,
       s.status
FROM ods.shipments s
JOIN ods.orders o         ON o.order_id = s.order_id
JOIN dw.dim_customer dc   ON dc.customer_id = o.customer_id
JOIN dw.dim_depot dd      ON dd.depot_id = o.depot_id
JOIN dw.dim_vehicle dv    ON dv.vehicle_id = s.vehicle_id
LEFT JOIN (
    SELECT oi.order_id,
           SUM(oi.qty * oi.unit_price) AS value,
           SUM(oi.qty)                 AS items,
           SUM(oi.qty * p.weight_kg)   AS weight
    FROM ods.order_items oi
    JOIN master.product p ON p.product_id = oi.product_id
    GROUP BY oi.order_id
) i ON i.order_id = o.order_id
WHERE s.dispatched_at IS NOT NULL
ON CONFLICT (shipment_id) DO UPDATE SET
    status        = EXCLUDED.status,
    lead_time_min = EXCLUDED.lead_time_min,
    delay_min     = EXCLUDED.delay_min,
    on_time       = EXCLUDED.on_time,
    loaded_at     = now();

REFRESH MATERIALIZED VIEW semantic.mv_cube_otif;
