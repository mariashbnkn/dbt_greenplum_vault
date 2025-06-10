Установка
``` bash
pip install --upgrade pip
pip install -r requirements.txt
dbt debug  # проверка подключения
dbt deps # add packages 
dbt run
```

Для сравнения эффективности
    1 - dbt
``` bash
dbt run -m tag:for_tests
dbt test --select raw_flight_data
```

    2 - Python/pandas
``` bash
python flight_data__for_tests_df.py
python flight_data__is_null__df.py
python flight_data__other_tests__df.py
```

    3 - Python/sqlalchemy
``` bash
python flight_data__for_tests_sa.py
python flight_data__other_tests_sa.py
```

