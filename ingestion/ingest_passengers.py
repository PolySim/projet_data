"""
Ingestion de l'entité `passengers` ; la couche Gold reste à compléter
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""

from datetime import date

import pandas as pd
from common import fetch_csv, get_connection, read_bronze_csv, validate_key

PASSENGER_COLS = [
    "first_name",
    "last_name",
    "gender",
    "nationality",
    "email",
    "birth_date",
    "signup_date",
]
FR_COLUMNS = {
    "id_passager": "passenger_id",
    "prenom": "first_name",
    "nom": "last_name",
    "genre": "gender",
    "nationalite": "nationality",
    "date_naissance": "birth_date",
    "date_inscription": "signup_date",
}


def ingest_bronze(day: date = None, init: bool = False):
    """Conserve séparément les snapshots EN et FR dans leur format source."""
    if not init and day is None:
        raise ValueError("Une date est nécessaire hors initialisation.")
    for language in ("en", "fr"):
        if init:
            fetch_csv("init", f"passengers_{language}.csv")
        else:
            fetch_csv("2025-09", f"passengers_{language}_{day.isoformat()}.csv")


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Consolide EN/FR avant l'upsert et la recherche des passagers absents."""
    frames = []
    for language, date_format in (("en", "%Y-%m-%d"), ("fr", "%d/%m/%Y")):
        frame = read_bronze_csv(f"passengers_{language}", day, init)
        if language == "fr":
            frame = frame.rename(columns=FR_COLUMNS)
            frame["gender"] = frame["gender"].replace(
                {"Homme": "Male", "Femme": "Female"}
            )
        if not frame["gender"].dropna().isin(["Male", "Female"]).all():
            raise ValueError(f"Valeur de genre inconnue dans la source {language}.")
        for column in ("birth_date", "signup_date"):
            frame[column] = pd.to_datetime(
                frame[column], format=date_format, errors="raise"
            ).dt.date
        frames.append(frame[["passenger_id"] + PASSENGER_COLS])

    df = pd.concat(frames, ignore_index=True)
    validate_key(df, "passenger_id")
    snapshot_date = date(2025, 8, 31) if init else day
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in PASSENGER_COLS)

    con = get_connection()
    try:
        con.execute("BEGIN TRANSACTION")
        create_silver_table(con)
        con.register("snapshot", df)
        con.execute(f"""
            INSERT INTO silver_passengers (
                passenger_id, {", ".join(PASSENGER_COLS)}, is_active, deleted_date,
                insert_timestamp, update_timestamp
            )
            SELECT passenger_id, {", ".join(PASSENGER_COLS)}, true, NULL, now(), now()
            FROM snapshot
            ON CONFLICT (passenger_id) DO UPDATE SET
                {set_clause}, is_active = true, deleted_date = NULL,
                update_timestamp = now()
        """)
        con.execute(
            """
            UPDATE silver_passengers SET
                is_active = false, deleted_date = ?, update_timestamp = now()
            WHERE is_active = true AND NOT EXISTS (
                SELECT 1 FROM snapshot
                WHERE snapshot.passenger_id = silver_passengers.passenger_id
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
    # TODO : reconstruire la/les table(s) de gold avec les données passengers à partir de silver_passengers
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
