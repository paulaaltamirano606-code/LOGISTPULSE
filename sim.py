"""Reglas de simulación de despachos (puras, sin I/O) para poder probarlas aparte.

Los atrasos dependen de: lluvia (dato de terceros en el Data Lake), transportista y
distancia. Así el Data Lake aporta señal real al modelo de ML y a los dashboards.
"""
from datetime import timedelta

PROMISE_HOURS = {"CORPORATE": 5, "PYME": 6, "RETAIL": 7}
# media de horas extra por transportista (calidad de servicio distinta)
CARRIER_EXTRA_H = {1: 0.2, 2: 0.6, 3: 1.4, 4: 0.9}
AVG_SPEED_KMH = 32.0


def transit_hours(rng, distance_km: float, rain_mm: float, carrier_id: int) -> float:
    base = distance_km / AVG_SPEED_KMH
    rain = rain_mm * 0.07
    carrier = rng.expovariate(1.0 / max(CARRIER_EXTRA_H.get(carrier_id, 0.5), 0.05))
    incident = rng.uniform(1.5, 6.0) if rng.random() < 0.07 else 0.0
    return max(0.2, base + rain + carrier + incident + rng.gauss(0, 0.15))


def simulate(rng, created_at, segment, distance_km, rain_mm, carrier_id):
    """Devuelve (promised_at, dispatched_at, delivered_at)."""
    promised_at = created_at + timedelta(hours=PROMISE_HOURS[segment])
    dispatched_at = created_at + timedelta(hours=rng.uniform(0.8, 4.5))
    delivered_at = dispatched_at + timedelta(hours=transit_hours(rng, distance_km, rain_mm, carrier_id))
    return promised_at, dispatched_at, delivered_at
