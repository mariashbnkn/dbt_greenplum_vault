SELECT 
  MIN(searchdate) AS min_search_date,
  MAX(searchdate) AS max_search_date
FROM test.flight_data;