-- Fourni : table de référence pour la conversion de devises.
-- Le dataset source contient des montants dans 3 devises (EUR/USD/GBP) sans taux de change
-- fourni nativement. Trouver/choisir des taux n'a pas d'intérêt pédagogique ici : on vous les
-- donne, à taux fixes, pour que vous puissiez vous concentrer sur l'ingestion et la modélisation.
--
-- Utilisation : joignez votre table de fait à dim_currency sur `currency` pour calculer une
-- colonne `amount_eur = amount * rate_to_eur` — colonne attendue dans fact_booking (cf. README,
-- section "Architecture attendue").

CREATE TABLE IF NOT EXISTS dim_currency (
    currency VARCHAR PRIMARY KEY,
    rate_to_eur DOUBLE,
    insert_timestamp TIMESTAMP DEFAULT now(),
    update_timestamp TIMESTAMP DEFAULT now()
);

INSERT INTO dim_currency (currency, rate_to_eur) VALUES
    ('EUR', 1.0),
    ('USD', 0.92),
    ('GBP', 1.16)
ON CONFLICT (currency) DO NOTHING;
