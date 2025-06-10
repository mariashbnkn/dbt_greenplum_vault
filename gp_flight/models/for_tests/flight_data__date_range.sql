-- models/date_range.sql
{{
  config(
    materialized='table',
    compresstype='zlib'
  )
}}

SELECT 
  MIN(searchdate) AS min_search_date,
  MAX(searchdate) AS max_search_date
FROM {{ source('test', 'flight_data') }}