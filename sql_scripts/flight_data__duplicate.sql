

select coalesce(sum(cnt), count(1)) from 
   (select
      count(1) as cnt
    from test.flight_data
    group by legid, searchdate
    having count(1) > 1
    ) t;