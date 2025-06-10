-- models/date_range.sql
{{
  config(
    materialized='table',
    compresstype='zlib'
  )
}}

SELECT 
  count(1)
FROM {{ source('test', 'flight_data') }}