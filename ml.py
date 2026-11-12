"""Módulo de Machine Learning: riesgo de atraso por despacho.

  DW (histórico entregado)  --> entrena modelo  --> puntúa despachos EN TRÁNSITO
  scores --> dw.ml_delay_scores (vuelve al DW)  y  métricas/predicciones --> Data Lake curated
"""
import io
import json
from datetime import datetime, timezone

import pandas as pd
from psycopg2.extras import execute_values
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from .common import CURATED, pg, put_bytes, query_df

NUM = ["distance_km", "rain_mm", "weight_kg", "items", "order_value_usd", "dispatch_hour", "sla_hours"]
CAT = ["segment", "depot", "carrier", "vehicle_kind", "weather"]


def build_pipeline() -> Pipeline:
    pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CAT)],
                            remainder="passthrough", sparse_threshold=0)
    return Pipeline([("pre", pre), ("clf", HistGradientBoostingClassifier(max_iter=150, learning_rate=0.08, random_state=7))])


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in NUM:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    for c in CAT:
        out[c] = out[c].fillna("NA").astype(str)
    return out


def main():
    df = query_df("SELECT * FROM semantic.v_shipments")
    hist = prepare(df[df["status"] == "DELIVERED"])
    live = prepare(df[df["status"] == "IN_TRANSIT"])
    if len(hist) < 200:
        raise SystemExit("[ml] pocos datos históricos: corre `make seed etl` primero")

    X, y = hist[NUM + CAT], (~hist["on_time"].astype(bool)).astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=7, stratify=y)
    model = build_pipeline().fit(X_tr, y_tr)
    auc = float(roc_auc_score(y_te, model.predict_proba(X_te)[:, 1]))

    version = datetime.now(timezone.utc).strftime("hgb-%Y%m%d-%H%M")
    metrics = {"model_version": version, "auc_holdout": round(auc, 4), "late_rate": round(float(y.mean()), 4),
               "n_train": int(len(X_tr)), "n_test": int(len(X_te)), "features": NUM + CAT}
    put_bytes(CURATED, "models/delay_risk/metrics.json", json.dumps(metrics, indent=2).encode())

    scored = 0
    if len(live):
        live = live.assign(p_late=model.predict_proba(live[NUM + CAT])[:, 1])
        rows = [(int(r.shipment_id), round(float(r.p_late), 4), version) for r in live.itertuples()]
        with pg() as conn, conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO dw.ml_delay_scores (shipment_id, p_late, model_version) VALUES %s
                ON CONFLICT (shipment_id) DO UPDATE SET p_late = EXCLUDED.p_late,
                    model_version = EXCLUDED.model_version, scored_at = now()""", rows)
        buf = io.BytesIO()
        live[["shipment_id", "p_late"]].to_parquet(buf, index=False)
        put_bytes(CURATED, "scores/delay_risk.parquet", buf.getvalue())
        scored = len(rows)
    print(f"[ml] {version}: AUC={auc:.3f} tasa de atraso={y.mean():.1%} | despachos puntuados en tránsito={scored}")


if __name__ == "__main__":
    main()
