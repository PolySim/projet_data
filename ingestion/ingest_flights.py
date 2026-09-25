"""
Ingestion de l'entité `flights` ; la couche Gold reste à compléter
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""

from datetime import date

from common import fetch_csv, get_connection, read_bronze_csv, validate_key

FLIGHT_COLS = [
    "flight_number",
    "airline",
    "origin_airport_id",
    "destination_airport_id",
    "flight_date",
    "departure_time",
    "arrival_time",
    "aircraft_type",
]


def ingest_bronze(day: date = None, init: bool = False):
    """Conserve le snapshot brut des vols, sans transformation."""
    if init:
        fetch_csv("init", "flights.csv")
    elif day is not None:
        fetch_csv("2025-09", f"flights_{day.isoformat()}.csv")
    else:
        raise ValueError("Une date est nécessaire hors initialisation.")


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id VARCHAR PRIMARY KEY,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Upsert du snapshot complet, puis désactivation des vols disparus."""
    df = read_bronze_csv("flights", day, init)
    validate_key(df, "flight_id")
    snapshot_date = date(2025, 8, 31) if init else day
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in FLIGHT_COLS)

    con = get_connection()
    try:
        con.execute("BEGIN TRANSACTION")
        create_silver_table(con)
        con.register("snapshot", df)
        con.execute(f"""
            INSERT INTO silver_flights (
                flight_id, {", ".join(FLIGHT_COLS)}, is_active, deleted_date,
                insert_timestamp, update_timestamp
            )
            SELECT flight_id, flight_number, airline, origin_airport_id,
                   destination_airport_id, CAST(flight_date AS DATE),
                   CAST(departure_time AS TIME), CAST(arrival_time AS TIME),
                   aircraft_type, true, NULL, now(), now()
            FROM snapshot
            ON CONFLICT (flight_id) DO UPDATE SET
                {set_clause}, is_active = true, deleted_date = NULL,
                update_timestamp = now()
        """)
        con.execute(
            """
            UPDATE silver_flights SET
                is_active = false, deleted_date = ?, update_timestamp = now()
            WHERE is_active = true AND NOT EXISTS (
                SELECT 1 FROM snapshot
                WHERE snapshot.flight_id = silver_flights.flight_id
            )
        """,
            [snapshot_date],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données flights à partir de silver_flights
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
