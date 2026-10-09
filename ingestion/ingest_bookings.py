"""
Ingestion de l'entité `bookings` en Bronze, Silver et Gold
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
    """Reconstruit le fait et son agrégation après les dimensions Gold."""
    con = get_connection()
    try:
        con.execute("BEGIN TRANSACTION")
        invalid_count = con.execute("""
            SELECT count(*)
            FROM silver_bookings b
            LEFT JOIN dim_flight f ON f.flight_id = b.flight_id
            LEFT JOIN dim_passenger p ON p.passenger_id = b.passenger_id
            LEFT JOIN dim_airport a ON a.airport_id = b.airport_id
            LEFT JOIN dim_currency c ON c.currency = b.currency
            WHERE f.flight_id IS NULL OR p.passenger_id IS NULL
               OR a.airport_id IS NULL OR c.rate_to_eur IS NULL
               OR c.rate_to_eur <= 0 OR b.amount IS NULL
               OR b.booking_date IS NULL OR f.airline IS NULL
               OR b.airport_id IS DISTINCT FROM f.origin_airport_id
        """).fetchone()[0]
        if invalid_count:
            raise ValueError(
                f"{invalid_count} réservation(s) avec référence, montant ou date invalide en Gold."
            )

        # Les taux fournis ont deux décimales : le produit exact en a quatre.
        # Les dimensions inactives restent accessibles aux réservations passées.
        con.execute("""
            CREATE OR REPLACE TABLE fact_booking AS
            SELECT b.*,
                   CAST(b.amount * CAST(c.rate_to_eur AS DECIMAL(9, 2))
                        AS DECIMAL(20, 4)) AS amount_eur
            FROM silver_bookings b
            JOIN dim_currency c ON c.currency = b.currency
        """)
        _refresh_daily_aggregation(con)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _refresh_daily_aggregation(con):
    """Conserve la première insertion de chaque groupe date/compagnie."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS agg_booking_daily_airline (
            booking_date DATE,
            airline VARCHAR,
            booking_count BIGINT,
            revenue_eur DECIMAL(38, 4),
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP,
            PRIMARY KEY (booking_date, airline)
        )
    """)
    con.execute("""
        CREATE TEMP TABLE daily_aggregation AS
        SELECT b.booking_date, f.airline, count(*) AS booking_count,
               sum(b.amount_eur) AS revenue_eur
        FROM fact_booking b
        JOIN dim_flight f ON f.flight_id = b.flight_id
        GROUP BY b.booking_date, f.airline
    """)
    con.execute("""
        INSERT INTO agg_booking_daily_airline (
            booking_date, airline, booking_count, revenue_eur,
            insert_timestamp, update_timestamp
        )
        SELECT booking_date, airline, booking_count, revenue_eur, now(), now()
        FROM daily_aggregation
        ON CONFLICT (booking_date, airline) DO UPDATE SET
            booking_count = excluded.booking_count,
            revenue_eur = excluded.revenue_eur,
            update_timestamp = now()
        WHERE agg_booking_daily_airline.booking_count IS DISTINCT FROM excluded.booking_count
           OR agg_booking_daily_airline.revenue_eur IS DISTINCT FROM excluded.revenue_eur
    """)
    # Un changement de compagnie peut faire disparaître un ancien groupe.
    con.execute("""
        DELETE FROM agg_booking_daily_airline a
        WHERE NOT EXISTS (
            SELECT 1 FROM daily_aggregation d
            WHERE d.booking_date = a.booking_date AND d.airline = a.airline
        )
    """)


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
