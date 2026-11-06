-- Idée de requêtes d'analyse pour valider votre modèle gold.

-- Les requêtes "métier" ne sont pas implémenté pour ne pas
-- donner trop d'indices sur le data modèle attendu. Mais vous
-- êtes libre d'essayer d'implémenter ces requêtes pour valider
-- votre conception.

-- 1. Chiffre d'affaires par mois et par devise
-- Le montant brut n'est additionné qu'au sein d'une même devise.
SELECT
    CAST(date_trunc('month', booking_date) AS DATE) AS booking_month,
    currency,
    count(*) AS booking_count,
    sum(amount) AS revenue_original_currency,
    round(sum(amount_eur), 2) AS revenue_eur
FROM fact_booking
GROUP BY booking_month, currency
ORDER BY booking_month, currency;

-- 2. Top 10 des compagnies aériennes par chiffre d'affaires
SELECT
    f.airline,
    count(*) AS booking_count,
    round(sum(b.amount_eur), 2) AS revenue_eur
FROM fact_booking b
JOIN dim_flight f ON f.flight_id = b.flight_id
GROUP BY f.airline
ORDER BY sum(b.amount_eur) DESC, f.airline
LIMIT 10;

-- 3. Répartition des réservations par classe de cabine
SELECT
    seat_class,
    count(*) AS booking_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS booking_percentage
FROM fact_booking
GROUP BY seat_class
ORDER BY booking_count DESC, seat_class;

-- 4. Volume de réservations par jour de la semaine
SELECT
    extract(isodow FROM booking_date) AS weekday_number,
    CASE extract(isodow FROM booking_date)
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
        WHEN 7 THEN 'Dimanche'
    END AS weekday_name,
    count(*) AS booking_count
FROM fact_booking
GROUP BY weekday_number, weekday_name
ORDER BY weekday_number;

-- 5. Top 10 des aéroports de départ générant le plus de réservations
SELECT
    a.airport_id,
    a.iata_code,
    a.airport_name,
    a.city,
    a.country,
    count(*) AS booking_count
FROM fact_booking b
JOIN dim_airport a ON a.airport_id = b.airport_id
GROUP BY a.airport_id, a.iata_code, a.airport_name, a.city, a.country
ORDER BY booking_count DESC, a.airport_id
LIMIT 10;

-- 6. Évolution quotidienne du nombre de réservations sur septembre
SELECT
    booking_date,
    count(*) AS booking_count
FROM fact_booking
WHERE booking_date >= DATE '2025-09-01'
  AND booking_date < DATE '2025-10-01'
GROUP BY booking_date
ORDER BY booking_date;

-- 7. Chiffre d'affaires normalisé en EUR
SELECT
    count(*) AS booking_count,
    sum(amount_eur) AS revenue_eur_exact,
    round(sum(amount_eur), 2) AS revenue_eur
FROM fact_booking;

-- 8. Durée moyenne de vol par compagnie
SELECT
    airline,
    count(*) AS flight_count,
    count(duration_minutes) AS flights_with_known_duration,
    round(avg(duration_minutes), 2) AS average_duration_minutes
FROM dim_flight
GROUP BY airline
ORDER BY avg(duration_minutes) DESC, airline;

-- 9. Chiffre d'affaires par tranche d'âge de passager
SELECT
    p.age_group,
    count(*) AS booking_count,
    round(sum(b.amount_eur), 2) AS revenue_eur
FROM fact_booking b
JOIN dim_passenger p ON p.passenger_id = b.passenger_id
GROUP BY p.age_group
ORDER BY CASE p.age_group
    WHEN '0–17' THEN 1
    WHEN '18–34' THEN 2
    WHEN '35–54' THEN 3
    WHEN '55+' THEN 4
    ELSE 5
END;
