-- ===================== CAPA SEMÁNTICA =====================
-- Vista denormalizada con nombres de negocio: es lo que consumen Grafana, el notebook y el autoservicio.
CREATE VIEW semantic.v_shipments AS
SELECT f.shipment_id,
       f.order_id,
       d.date,
       d.year,
       d.quarter,
       d.month,
       d.month_name,
       d.day_of_week,
       d.is_weekend,
       f.dispatch_hour,
       c.customer_id,
       c.name              AS customer,
       c.segment,
       z.name              AS zone,
       z.city              AS zone_city,
       z.population        AS zone_population,
       z.avg_income_usd    AS zone_avg_income_usd,
       dp.name             AS depot,
       dp.city             AS depot_city,
       v.vehicle_id,
       v.kind              AS vehicle_kind,
       ca.name             AS carrier,
       ca.sla_hours,
       w.rain_mm,
       w.condition         AS weather,
       f.order_value_usd,
       f.items,
       f.weight_kg,
       f.distance_km,
       f.lead_time_min,
       f.delay_min,
       f.on_time,
       f.status,
       s.p_late
FROM dw.fact_shipment f
JOIN dw.dim_date     d  ON d.date_key     = f.date_key
JOIN dw.dim_customer c  ON c.customer_key = f.customer_key
LEFT JOIN dw.dim_zone z ON z.zone_key     = c.zone_key
JOIN dw.dim_depot    dp ON dp.depot_key   = f.depot_key
JOIN dw.dim_vehicle  v  ON v.vehicle_key  = f.vehicle_key
LEFT JOIN dw.dim_carrier ca ON ca.carrier_key = f.carrier_key
LEFT JOIN dw.dim_weather w  ON w.date_key = f.date_key AND w.city = dp.city
LEFT JOIN dw.ml_delay_scores s ON s.shipment_id = f.shipment_id;

-- ===================== CUBO OLAP (precalculado) =====================
-- GROUPING SETS = los "cortes" del cubo. Las columnas NULL indican nivel agregado.
CREATE MATERIALIZED VIEW semantic.mv_cube_otif AS
SELECT year,
       month_name AS month,
       depot,
       carrier,
       segment,
       COUNT(*)                                                         AS shipments,
       COUNT(*) FILTER (WHERE on_time)                                  AS on_time_shipments,
       ROUND(100.0 * COUNT(*) FILTER (WHERE on_time) / NULLIF(COUNT(on_time), 0), 2) AS otif_pct,
       ROUND(AVG(delay_min)::numeric, 1)                                AS avg_delay_min,
       ROUND(SUM(order_value_usd), 2)                                   AS revenue_usd
FROM semantic.v_shipments
WHERE status = 'DELIVERED'
GROUP BY GROUPING SETS (
    (year, month_name, depot, carrier, segment),
    (year, month_name),
    (depot),
    (carrier),
    (segment),
    ()
);

-- ===================== DATA MARTS =====================
CREATE VIEW mart_ops.daily_depot_kpis AS
SELECT date,
       depot,
       COUNT(*)                                   AS shipments,
       ROUND(100.0 * COUNT(*) FILTER (WHERE on_time) / NULLIF(COUNT(on_time), 0), 2) AS otif_pct,
       ROUND(AVG(delay_min)::numeric, 1)          AS avg_delay_min,
       ROUND(AVG(distance_km)::numeric, 1)        AS avg_distance_km
FROM semantic.v_shipments
WHERE status = 'DELIVERED'
GROUP BY date, depot;

CREATE VIEW mart_ops.delay_risk_in_transit AS
SELECT shipment_id, depot, carrier, customer, vehicle_id, distance_km, rain_mm, p_late
FROM semantic.v_shipments
WHERE status = 'IN_TRANSIT'
ORDER BY p_late DESC NULLS LAST;

CREATE VIEW mart_ops.fleet_health_now AS
SELECT DISTINCT ON (vehicle_id) vehicle_id, ts, speed_kmh, battery_pct, temp_c, status
FROM ods.amr_telemetry
ORDER BY vehicle_id, ts DESC;

CREATE VIEW mart_commercial.customer_service_level AS
SELECT customer,
       segment,
       zone,
       COUNT(*)                                   AS shipments,
       ROUND(SUM(order_value_usd), 2)             AS revenue_usd,
       ROUND(100.0 * COUNT(*) FILTER (WHERE on_time) / NULLIF(COUNT(on_time), 0), 2) AS otif_pct,
       ROUND(AVG(delay_min)::numeric, 1)          AS avg_delay_min
FROM semantic.v_shipments
WHERE status = 'DELIVERED'
GROUP BY customer, segment, zone;

CREATE VIEW mart_commercial.zone_demand AS
SELECT zone, zone_city, zone_population, zone_avg_income_usd,
       COUNT(*) AS shipments,
       ROUND(SUM(order_value_usd), 2) AS revenue_usd
FROM semantic.v_shipments
GROUP BY zone, zone_city, zone_population, zone_avg_income_usd;
