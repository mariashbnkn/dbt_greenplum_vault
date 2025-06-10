CREATE OR REPLACE FUNCTION validate_flight_data()
RETURNS TABLE (
    test_name TEXT,
    error_count INTEGER,
    error_details TEXT
) AS $$
BEGIN
    -- 1. Проверка NULL в legid
    RETURN QUERY
    SELECT 
        'legid_not_null'::text AS test_name,
        COUNT(1)::int AS error_count,
        'Найдены записи с NULL в legid'::text AS error_details
    FROM raw.raw_flight_data
    WHERE legid IS NULL;

    -- 2. Проверка flightdate >= searchdate
    RETURN QUERY
    SELECT 
        'flightdate_after_searchdate'::text AS test_name,
        COUNT(*)::int AS error_count,
        'Найдены записи где flightdate раньше searchdate'::text AS error_details
    FROM raw.raw_flight_data
    WHERE flightdate < searchdate;

    -- 3. Проверка startingairport != destinationairport
    RETURN QUERY
    SELECT 
        'airports_different'::text AS test_name,
        COUNT(*)::int AS error_count,
        'Найдены записи с одинаковыми startingairport и destinationairport'::text AS error_details
    FROM raw.raw_flight_data
    WHERE startingairport = destinationairport;

    -- 4. Проверка totalfare >= basefare
    RETURN QUERY
    SELECT 
        'totalfare_ge_basefare'::text AS test_name,
        COUNT(*)::int AS error_count,
        'Найдены записи где totalfare меньше basefare'::text AS error_details
    FROM raw.raw_flight_data
    WHERE totalfare < basefare;

    RETURN;
END;
$$ LANGUAGE plpgsql;	