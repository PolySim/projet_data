"""
Utilitaires partagés : téléchargement brut avec cache local et connexion DuckDB.
"""

import os
import shutil
import tempfile
from pathlib import Path
from urllib.request import urlopen

import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
RAW_BASE = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main"


def fetch_csv(subdir: str, filename: str) -> pd.DataFrame:
    """Conserve le CSV brut si nécessaire, puis le lit depuis le disque."""
    path = Path(BRONZE_DIR) / subdir / filename
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with urlopen(f"{RAW_BASE}/{subdir}/{filename}", timeout=30) as response:
                with tempfile.NamedTemporaryFile(
                    dir=path.parent, suffix=".tmp", delete=False
                ) as temporary_file:
                    temporary_path = Path(temporary_file.name)
                    shutil.copyfileobj(response, temporary_file)
            os.replace(temporary_path, path)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    return pd.read_csv(path)


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def read_bronze_csv(entity: str, day=None, init: bool = False) -> pd.DataFrame:
    """Lit uniquement Bronze ; une source absente doit d'abord être ingérée."""
    if init:
        path = Path(BRONZE_DIR) / "init" / f"{entity}.csv"
    elif day is not None:
        path = Path(BRONZE_DIR) / "2025-09" / f"{entity}_{day.isoformat()}.csv"
    else:
        raise ValueError("Une date est nécessaire hors initialisation.")
    return pd.read_csv(path, dtype=str)


def validate_key(df: pd.DataFrame, key: str):
    """Refuse un snapshot ambigu avant toute modification de l'entrepôt."""
    if df[key].isna().any() or df[key].str.strip().eq("").any():
        raise ValueError(f"La clé {key} contient des valeurs absentes.")
    if df[key].duplicated().any():
        raise ValueError(f"La clé {key} contient des doublons.")
