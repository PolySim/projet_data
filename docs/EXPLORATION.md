# Exploration des données

## Primary keys

| Source | Clé unique observée | Init | 01/09 | 02/09 | 03/09 | Nature du fichier |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Aéroports | `airport_id` | 80 | 80 | 80 | 80 | État complet |
| Vols | `flight_id` | 350 | 349 | 350 | 350 | État complet |
| Passagers EN | `passenger_id` | 900 | 910 | 913 | 922 | État complet |
| Passagers FR | `id_passager` | 900 | 907 | 913 | 915 | État complet |
| Réservations | `booking_id` | 400 | 955 | 943 | 1 025 | Historique initial, puis nouveautés quotidiennes |

- Les aéroports ne changent pas dans cet échantillon. Le README source annonce
  un ajout et une correction sur le mois, sans suppression.
- Les vols présentent un ajout et deux disparitions le 1er septembre, un ajout
  et deux modifications de date le 2, puis une modification d'horaires le 3.
  Un nombre de lignes stable ne signifie donc pas une absence de changements.
- Les passagers croissent chaque jour ; une adresse e-mail EN est corrigée le 3.
  Le README source annonce aussi des disparitions plus tard dans le mois.
- Aucun doublon de clé dans les fichiers examinés, aucun chevauchement des IDs
  passagers EN/FR, et aucun ID de réservation répété entre ces quatre périodes.
- Chaque réservation examinée référence un passager, un vol et un aéroport
  présents dans les fichiers de sa période. Son `airport_id` correspond au
  `origin_airport_id` du vol.


Le fait est la réservation : une ligne de `fact_booking` représente un événement
identifié par `booking_id`, avec un montant, une devise et une date de réservation.
Les aéroports, vols et passagers sont des dimensions décrivant cet événement.
Leurs états complets seront chargés par upsert sur la clé source, en conservant
la valeur courante (SCD1). Une clé absente d'un snapshot complet sera désactivée
(`is_active = false`, `deleted_date`), jamais supprimée : les anciennes
réservations doivent conserver leurs références. Les deux sources passagers
seront consolidées avant de rechercher les absents. Les réservations seront
ajoutées une seule fois par `booking_id` ; leur absence d'un fichier quotidien
ne constitue pas une suppression. Une réapparition de dimension la réactivera
et remettra `deleted_date` à `NULL`.


Les passagers doivent être harmonisés en Silver : `id_passager` → `passenger_id`,
`prenom` → `first_name`, `nom` → `last_name`, `genre` → `gender`,
`nationalite` → `nationality`, `date_naissance` → `birth_date`,
`date_inscription` → `signup_date`. `email` existe dans les deux sources.
Les dates FR sont au format `DD/MM/YYYY`, les dates EN au format `YYYY-MM-DD`.
Les valeurs `Homme`/`Femme` seront normalisées en `Male`/`Female` avant union.
