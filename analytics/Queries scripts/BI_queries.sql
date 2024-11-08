-- 1. Routes with Stopovers or Distance Greater Than 1000 km
SELECT
    a.name AS airline_name,
    r.flight_number,
    r.source_airport_id,
    r.destination_airport_id,
    r.stopovers,
    r.distance_km
FROM
    fact_routes r
JOIN
    dim_airlines a ON r.airline_id = a.airline_id
WHERE
    r.stopovers > 0 OR r.distance_km > 1000
ORDER BY
    r.distance_km DESC;

-- 2. Routes with Fewer Than 3 Airlines and Average Distance
SELECT
    r.source_airport_id,
    r.destination_airport_id,
    COUNT(r.airline_id) AS airline_count,
    AVG(r.distance_km) AS avg_distance
FROM
    fact_routes r
GROUP BY
    r.source_airport_id, r.destination_airport_id
HAVING
    airline_count < 3
ORDER BY
    airline_count ASC, avg_distance DESC;

-- 3. Active Airlines with More Than 1 Stopover or Distance Greater Than 2000 km
SELECT
    a.name AS airline_name,
    r.flight_number,
    r.source_airport_id,
    r.destination_airport_id,
    r.stopovers,
    r.distance_km
FROM
    fact_routes r
JOIN
    dim_airlines a ON r.airline_id = a.airline_id
WHERE
    a.active = TRUE
    AND (r.stopovers > 1 OR r.distance_km > 2000)
ORDER BY
    r.distance_km DESC, r.stopovers DESC;

-- 4. Airports with Fewer Than 10 Routes (In- and Outbound)
SELECT
    ap.name AS airport_name,
    ap.city,
    ap.country,
    COUNT(r.source_airport_id) + COUNT(r.destination_airport_id) AS total_routes
FROM
    dim_airports ap
LEFT JOIN
    fact_routes r ON ap.airport_id = r.source_airport_id OR ap.airport_id = r.destination_airport_id
GROUP BY
    ap.airport_id, ap.name, ap.city, ap.country
HAVING
    total_routes < 10
ORDER BY
    total_routes ASC;

-- 5. Airports with Fewer Than 10 Routes (In- and Outbound) in Descending Order
SELECT
    ap.name AS airport_name,
    ap.city,
    ap.country,
    COUNT(r.source_airport_id) + COUNT(r.destination_airport_id) AS total_routes
FROM
    dim_airports ap
LEFT JOIN
    fact_routes r ON ap.airport_id = r.source_airport_id OR ap.airport_id = r.destination_airport_id
GROUP BY
    ap.airport_id, ap.name, ap.city, ap.country
HAVING
    COUNT(r.source_airport_id) + COUNT(r.destination_airport_id) < 10
ORDER BY
    total_routes DESC;
