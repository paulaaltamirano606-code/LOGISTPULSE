"""Reglas de alerta en tiempo real (puras, probables sin Kafka)."""

BATTERY_MIN = 15.0
TEMP_MAX = 45.0
SPEED_LIMIT = {"AMR": 5.0, "VAN": 80.0, "TRUCK": 70.0}


def kind_of(vehicle_id: str) -> str:
    if "-AMR-" in vehicle_id:
        return "AMR"
    return "TRUCK" if "-TRK-" in vehicle_id else "VAN"


def evaluate(msg: dict) -> list[tuple[str, str]]:
    """Devuelve [(tipo_alerta, detalle)] para un mensaje de telemetría."""
    out = []
    if msg["battery_pct"] < BATTERY_MIN and msg.get("status") != "CHARGING":
        out.append(("LOW_BATTERY", f"batería {msg['battery_pct']}%"))
    if msg["temp_c"] > TEMP_MAX:
        out.append(("OVERHEAT", f"temperatura {msg['temp_c']} °C"))
    limit = SPEED_LIMIT[kind_of(msg["vehicle_id"])]
    if msg["speed_kmh"] > limit:
        out.append(("OVERSPEED", f"{msg['speed_kmh']} km/h (límite {limit})"))
    return out
