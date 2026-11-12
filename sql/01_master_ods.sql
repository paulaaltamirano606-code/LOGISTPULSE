-- ===================== MASTER DATA =====================
CREATE TABLE master.zone (
    zone_id  int  PRIMARY KEY,
    name     text NOT NULL,
    city     text NOT NULL
);

CREATE TABLE master.depot (
    depot_id int  PRIMARY KEY,
    name     text NOT NULL,
    city     text NOT NULL
);

CREATE TABLE master.carrier (
    carrier_id int  PRIMARY KEY,
    name       text NOT NULL
);

CREATE TABLE master.vehicle (
    vehicle_id text PRIMARY KEY,
    kind       text NOT NULL CHECK (kind IN ('AMR','VAN','TRUCK')),
    depot_id   int  NOT NULL REFERENCES master.depot,
    carrier_id int  REFERENCES master.carrier          -- NULL para AMR (robots de bodega)
);

CREATE TABLE master.customer (
    customer_id int  PRIMARY KEY,
    name        text NOT NULL,
    segment     text NOT NULL CHECK (segment IN ('RETAIL','PYME','CORPORATE')),
    zone_id     int  NOT NULL REFERENCES master.zone
);

CREATE TABLE master.product (
    product_id int  PRIMARY KEY,
    name       text NOT NULL,
    category   text NOT NULL,
    weight_kg  numeric(8,2) NOT NULL CHECK (weight_kg > 0)
);

-- ============ ODS (datos organizacionales: pedidos y despachos) ============
CREATE TABLE ods.orders (
    order_id    bigint PRIMARY KEY,
    customer_id int  NOT NULL REFERENCES master.customer,
    depot_id    int  NOT NULL REFERENCES master.depot,
    created_at  timestamptz NOT NULL,
    promised_at timestamptz NOT NULL,
    status      text NOT NULL CHECK (status IN ('CREATED','IN_TRANSIT','DELIVERED','CANCELLED')),
    CHECK (promised_at > created_at)
);

CREATE TABLE ods.order_items (
    order_id   bigint NOT NULL REFERENCES ods.orders,
    product_id int    NOT NULL REFERENCES master.product,
    qty        int    NOT NULL CHECK (qty > 0),
    unit_price numeric(10,2) NOT NULL CHECK (unit_price >= 0),
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE ods.shipments (
    shipment_id   bigint PRIMARY KEY,
    order_id      bigint NOT NULL UNIQUE REFERENCES ods.orders,
    vehicle_id    text   NOT NULL REFERENCES master.vehicle,
    dispatched_at timestamptz,
    delivered_at  timestamptz,
    distance_km   numeric(7,2) NOT NULL CHECK (distance_km > 0),
    status        text NOT NULL CHECK (status IN ('PENDING','IN_TRANSIT','DELIVERED','FAILED'))
);

-- ============ ODS (streaming: telemetría reciente y alertas) ============
CREATE TABLE ods.amr_telemetry (
    ts          timestamptz NOT NULL,
    vehicle_id  text NOT NULL,
    x           real,
    y           real,
    speed_kmh   real,
    battery_pct real,
    temp_c      real,
    status      text
);
CREATE INDEX amr_telemetry_ts_idx ON ods.amr_telemetry (ts DESC);
CREATE INDEX amr_telemetry_vehicle_ts_idx ON ods.amr_telemetry (vehicle_id, ts DESC);

CREATE TABLE ods.alerts (
    alert_id   bigserial PRIMARY KEY,
    ts         timestamptz NOT NULL DEFAULT now(),
    vehicle_id text NOT NULL,
    alert_type text NOT NULL,
    detail     text
);
CREATE INDEX alerts_ts_idx ON ods.alerts (ts DESC);

-- ============ STAGING (aterrizaje desde el Data Lake / terceros / demográficos) ============
CREATE TABLE staging.zone_demo (
    zone_id               int,
    population            int,
    avg_income_usd        numeric,
    vehicle_household_pct numeric
);

CREATE TABLE staging.carrier_sla (
    carrier_id      int,
    sla_hours       numeric,
    sla_penalty_usd numeric
);

CREATE TABLE staging.weather (
    date      date,
    city      text,
    rain_mm   numeric,
    condition text
);
