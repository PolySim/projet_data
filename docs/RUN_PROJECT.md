# Run Project

Ce fichier indique comment exécuter le projet. 

## Prérequis

```sh
python3 -m venv venv
source venv/bin/activate      
pip install -r requirements.txt
```

## Bronze

Si vous avez déjà des données brutes et que vous souhaitez les traiter à nouveau commencez par faire `rm -rf bronze` 

```sh
python3 ingestion/run_month.py --bronze-only
```
