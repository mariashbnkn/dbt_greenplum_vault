-- models/date_range.sql
{{
  config(
    materialized='table',
    compresstype='zlib'
  )
}}

select coalesce(sum(cnt), count(1)) from 
   (select
      count(1) as cnt
    from {{ source('test', 'flight_data') }}
    group by legid, searchdate
    having count(1) > 1
    ) t