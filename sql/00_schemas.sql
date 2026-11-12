-- LOGISTPULSE Lakehouse :: capas del diagrama Data Warehouse vs Data Lake
-- master            -> Master Data
-- ods               -> Operational Data Store (+ streaming reciente)
-- staging           -> zona de aterrizaje del ETL en lotes
-- dw                -> Data Warehouse (modelo estrella)
-- mart_ops / mart_commercial -> Data Marts
-- semantic          -> Capa semántica + cubo OLAP (vista materializada)
CREATE SCHEMA IF NOT EXISTS master;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart_ops;
CREATE SCHEMA IF NOT EXISTS mart_commercial;
CREATE SCHEMA IF NOT EXISTS semantic;
