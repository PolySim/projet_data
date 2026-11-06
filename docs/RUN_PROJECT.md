# Exécuter le projet

Toutes les commandes se lancent depuis la racine du dépôt, avec Python 3.10 ou
ultérieur.

## Installation

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Pipeline complet

```sh
python ingestion/run_month.py
```

Exécute Bronze, Silver puis Gold pour l’initialisation et les 30 jours de
septembre 2025. La dernière ligne est `2025-09-30 ingéré.`. Les CSV déjà présents
sont réutilisés sans téléchargement. Le rejeu ne duplique pas les réservations.

## Exécuter une couche

```sh
python ingestion/run_month.py --bronze-only
python ingestion/run_month.py --silver-only
python ingestion/run_month.py --gold-only
```

Bronze télécharge et conserve les 155 CSV bruts. Silver nécessite ces fichiers
locaux et rejoue tous les snapshots dans l’ordre chronologique. Gold nécessite
Silver et initialise les taux de conversion avant de reconstruire le modèle.
Les options sont exclusives : lancer les commandes séparément.

Après intégration de la colonne passager `source`, rejouer Silver puis Gold :
le schéma existant est migré automatiquement et le rejeu complet renseigne la
provenance des passagers actifs comme inactifs. Il est inutile de supprimer
l’entrepôt ou le cache Bronze pour un rejeu normal. Si les exports distants ont
été corrigés, sauvegarder puis écarter le cache concerné pour les retélécharger.

## Vérifier le résultat

```sh
python - <<'PY'
import duckdb
with duckdb.connect("warehouse.duckdb", read_only=True) as con:
    print(con.execute("SELECT count(*), sum(amount_eur) FROM fact_booking").fetchall())
    print(con.execute("SELECT source, count(*) FROM dim_passenger GROUP BY source ORDER BY source").fetchall())
PY
```

Avec le jeu de données validé : **38 930 réservations**, pour
**13 816 124,3440 EUR**, initialisation d’août incluse. Les passagers doivent tous
avoir une source `EN` ou `FR`. Fermer toute autre connexion à l’entrepôt avant de
relancer les écritures pour éviter les conflits de verrouillage.

Le rendu contient le code et `DOCUMENTATION.md` (dont le diagramme du modèle).
Ne pas inclure `venv/`, `bronze/`, `warehouse.duckdb` ni les fichiers `__pycache__`.
