-- ===================== DATA WAREHOUSE (modelo estrella) =====================
CREATE TABLE dw.dim_date (
    date_key    int  PRIMARY KEY,           -- YYYYMMDD
    date        date NOT NULL UNIQUE,
    year        int  NOT NULL,
    quarter     int  NOT NULL,
    month       int  NOT NULL,
    month_name  text NOT NULL,
    day_of_week int  NOT NULL,              -- 1=lunes ... 7=domingo
    is_weekend  boolean NOT NULL
);

INSERT INTO dw.dim_date
SELECT to_char(d, 'YYYYMMDD')::int,
       d::date,
       EXTRACT(year FROM d)::int,
       EXTRACT(quarter FROM d)::int,
       EXTRACT(month FROM d)::int,
       to_char(d, 'YYYY-MM'),
       EXTRACT(isodow FROM d)::int,
       EXTRACT(isodow FROM d) >= 6
FROM generate_series(current_date - 400, current_date + 30, interval '1 day') AS d;

CREATE TABLE dw.dim_zone (
    zone_key              serial PRIMARY KEY,
    zone_id               int UNIQUE NOT NULL,
    name                  text,
    city                  text,
    population            int,              -- Datos demográficos (Data Lake)
    avg_income_usd        numeric,
    vehicle_household_pct numeric
);

CREATE TABLE dw.dim_customer (
    customer_key serial PRIMARY KEY,
    customer_id  int UNIQUE NOT NULL,
    name         text,
    segment      text,
    zone_key     int REFERENCES dw.dim_zone
);

CREATE TABLE dw.dim_depot (
    depot_key serial PRIMARY KEY,
    depot_id  int UNIQUE NOT NULL,
    name      text,
    city      text
);

CREATE TABLE dw.dim_carrier (
    carrier_key     serial PRIMARY KEY,
    carrier_id      int UNIQUE NOT NULL,
    name            text,
    sla_hours       numeric,                -- Datos de terceros (Data Lake)
    sla_penalty_usd numeric
);

CREATE TABLE dw.dim_vehicle (
    vehicle_key serial PRIMARY KEY,
    vehicle_id  text UNIQUE NOT NULL,
    kind        text,
    depot_key   int REFERENCES dw.dim_depot,
    carrier_key int REFERENCES dw.dim_carrier
);

CREATE TABLE dw.dim_weather (
    date_key  int  NOT NULL,
    city      text NOT NULL,
    rain_mm   numeric,
    condition text,
    PRIMARY KEY (date_key, city)
);

CREATE TABLE dw.fact_shipment (
    shipment_id     bigint PRIMARY KEY,
    order_id        bigint NOT NULL,
    date_key        int NOT NULL REFERENCES dw.dim_date,   -- fecha de despacho
    dispatch_hour   smallint,
    customer_key    int NOT NULL REFERENCES dw.dim_customer,
    depot_key       int NOT NULL REFERENCES dw.dim_depot,
    vehicle_key     int NOT NULL REFERENCES dw.dim_vehicle,
    carrier_key     int REFERENCES dw.dim_carrier,
    order_value_usd numeric(12,2),
    items           int,
    weight_kg       numeric(10,2),
    distance_km     numeric(7,2),
    lead_time_min   numeric(10,1),           -- creación -> entrega
    delay_min       numeric(10,1),           -- entrega - promesa (>0 = tarde)
    on_time         boolean,                 -- NULL si aún no se entrega
    status          text NOT NULL,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX fact_shipment_date_idx ON dw.fact_shipment (date_key);

-- Salida del módulo de Machine Learning devuelta al DW (flecha DW <-> ML del diagrama)
CREATE TABLE dw.ml_delay_scores (
    shipment_id   bigint PRIMARY KEY,
    p_late        numeric(5,4) NOT NULL,
    model_version text NOT NULL,
    scored_at     timestamptz NOT NULL DEFAULT now()
);
