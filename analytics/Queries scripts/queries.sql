-- 1. Top 3 Airlines with Most Routes Per Country
WITH airline_route_counts AS (
    SELECT 
        r.airline_id, 
        a.country, 
        COUNT(*) AS route_count
    FROM fact_routes r
    JOIN dim_airports a ON r.source_airport_id = a.airport_id
    GROUP BY r.airline_id, a.country
    
    UNION ALL

    SELECT 
        r.airline_id, 
        CASE
            WHEN r.destination_airport_id != r.source_airport_id THEN a.country
            ELSE NULL
        END AS country, 
        COUNT(*) AS route_count
    FROM fact_routes r
    JOIN dim_airports a ON r.destination_airport_id = a.airport_id
    GROUP BY r.airline_id, a.country, r.destination_airport_id, r.source_airport_id
)
SELECT 
    country, 
    airline_id, 
    route_count
FROM (
    SELECT 
        country, 
        airline_id, 
        route_count,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY route_count DESC) AS rank
    FROM airline_route_counts
) ranked_routes
WHERE rank <= 3;

-- 2. Airports with Most Codeshare Routes
SELECT 
    a.name AS airport_name,
    a.city,
    a.country,
    COUNT(r.flight_number) AS codeshare_routes
FROM fact_routes r
JOIN dim_airports a ON r.source_airport_id = a.airport_id
WHERE r.codeshare = TRUE
GROUP BY a.name, a.city, a.country
ORDER BY codeshare_routes DESC;

-- 3. Longest Non-Stop Route
SELECT 
    r.airline_id,
    sa.name AS source_airport,
    da.name AS destination_airport,
    r.distance_km
FROM fact_routes r
JOIN dim_airports sa ON r.source_airport_id = sa.airport_id
JOIN dim_airports da ON r.destination_airport_id = da.airport_id
WHERE r.stopovers = 0
ORDER BY r.distance_km DESC
LIMIT 1;

-- 4. Airport with the Most Unique Airlines
SELECT 
    a.name AS airport_name,
    a.city,
    COUNT(DISTINCT r.airline_id) AS unique_airlines
FROM fact_routes r
JOIN dim_airports a ON r.source_airport_id = a.airport_id
GROUP BY a.name, a.city
ORDER BY unique_airlines DESC
LIMIT 1;

-- 5. Airline with the Longest Route
SELECT al.name, MAX(r.distance_km) AS max_distance
FROM fact_routes r
JOIN dim_airlines al ON r.airline_id = al.airline_id
GROUP BY al.name
ORDER BY max_distance DESC
LIMIT 1;

-- 6. Cities with Multiple Types of Airports
SELECT 
    city,
    COUNT(DISTINCT airport_type) AS transport_types_count
FROM dim_airports
GROUP BY city
HAVING COUNT(DISTINCT airport_type) > 1
ORDER BY transport_types_count DESC;

-- 7. Top 10 Airports by Number of Airlines and Routes
WITH airport_airline_counts AS (
    SELECT 
        r.source_airport_id AS airport_id,
        COUNT(DISTINCT r.airline_id) AS airline_count
    FROM fact_routes r
    GROUP BY r.source_airport_id
),
airport_route_counts AS (
    SELECT 
        r.source_airport_id AS airport_id,
        COUNT(*) AS route_count
    FROM fact_routes r
    GROUP BY r.source_airport_id
)
SELECT 
    a.airport_id, 
    a.name AS airport_name,
    a.city,
    a.country,
    COALESCE(aa.airline_count, 0) AS airline_count,
    COALESCE(ar.route_count, 0) AS route_count
FROM dim_airports a
LEFT JOIN airport_airline_counts aa ON a.airport_id = aa.airport_id
LEFT JOIN airport_route_counts ar ON a.airport_id = ar.airport_id
ORDER BY airline_count DESC, route_count DESC
LIMIT 10;
