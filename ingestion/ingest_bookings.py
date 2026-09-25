"""
Ingestion de l'entité `bookings` ; la couche Gold reste à compléter
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""

from datetime import date

from common import fetch_csv, get_connection, read_bronze_csv, validate_key


def ingest_bronze(day: date = None, init: bool = False):
    """Conserve le fichier brut des réservations initiales ou du jour."""
    if init:
        fetch_csv("init", "bookings.csv")
    elif day is not None:
        fetch_csv("2025-09", f"bookings_{day.isoformat()}.csv")
    else:
        raise ValueError("Une date est nécessaire hors initialisation.")


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DECIMAL(18, 2),
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Ajoute les événements du jour ; un rejeu conserve les réservations connues."""
    df = read_bronze_csv("bookings", day, init)
    validate_key(df, "booking_id")
    con = get_connection()
    try:
        con.execute("BEGIN TRANSACTION")
        create_silver_table(con)
        con.register("snapshot", df)
        con.execute("""
            INSERT INTO silver_bookings (
                booking_id, passenger_id, flight_id, airport_id, seat_class,
                amount, currency, booking_date, booking_channel,
                insert_timestamp, update_timestamp
            )
            SELECT booking_id, passenger_id, flight_id, airport_id, seat_class,
                   CAST(amount AS DECIMAL(18, 2)), currency, CAST(booking_date AS DATE),
                   booking_channel, now(), now()
            FROM snapshot
            ON CONFLICT (booking_id) DO NOTHING
        """)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données booking à partir de silver_bookings.
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
