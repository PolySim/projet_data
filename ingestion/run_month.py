"""
Fourni : rejoue l'ingestion sur tout le mois de septembre 2025, jour par jour, comme si le
pipeline avait tourné quotidiennement. Vous n'avez pas besoin d'attendre le vrai calendrier
pour tester votre pipeline sur l'ensemble de la période — lancez simplement ce script une fois
vos fonctions bronze/silver/gold complétées dans les 3 scripts d'ingestion restants.

Ordre d'exécution, pour l'init comme pour chaque jour : bronze des 4 entités, PUIS silver des 4
entités, PUIS gold des 4 entités — pas entité par entité. Pourquoi : le gold des réservations a
besoin que le silver/gold des vols du même jour soit déjà à jour (cohérence entre `airport_id` et
l'aéroport de départ du vol réservé). C'est exactement le genre de dépendance qu'un orchestrateur
comme Dagster gère pour vous (cf. section Bonus du README).
"""

import argparse
import os
from datetime import date, timedelta

import ingest_airports
import ingest_bookings
import ingest_flights
import ingest_passengers
from common import get_connection

START = date(2025, 9, 1)
N_DAYS = 30

ENTITIES = [ingest_airports, ingest_flights, ingest_passengers, ingest_bookings]


def init_dim_currency():
    sql_path = os.path.join(
        os.path.dirname(__file__), "..", "sql", "init", "dim_currency.sql"
    )
    con = get_connection()
    con.execute(open(sql_path).read())
    con.close()


def run(bronze_only: bool = False, silver_only: bool = False):
    if bronze_only and silver_only:
        raise ValueError("Choisir une seule couche à exécuter.")
    if not bronze_only and not silver_only:
        init_dim_currency()

    if not silver_only:
        for entity in ENTITIES:
            entity.ingest_bronze(init=True)
    if not bronze_only:
        for entity in ENTITIES:
            entity.ingest_silver(init=True)
    if not bronze_only and not silver_only:
        for entity in ENTITIES:
            entity.ingest_gold()
    print("Init ingérée.")

    for i in range(N_DAYS):
        day = START + timedelta(days=i)

        if not silver_only:
            for entity in ENTITIES:
                entity.ingest_bronze(day)
        if not bronze_only:
            for entity in ENTITIES:
                entity.ingest_silver(day)
        if not bronze_only and not silver_only:
            for entity in ENTITIES:
                entity.ingest_gold()

        print(f"{day.isoformat()} ingéré.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion du mois de septembre 2025.")
    layers = parser.add_mutually_exclusive_group()
    layers.add_argument(
        "--bronze-only",
        action="store_true",
        help="Télécharger uniquement les CSV bruts.",
    )
    layers.add_argument(
        "--silver-only",
        action="store_true",
        help="Charger uniquement Silver depuis les CSV Bronze locaux.",
    )
    args = parser.parse_args()
    run(bronze_only=args.bronze_only, silver_only=args.silver_only)
