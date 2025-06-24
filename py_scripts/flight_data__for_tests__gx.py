from datetime import datetime
import os
from dotenv import load_dotenv
import great_expectations as gx
import great_expectations.expectations as gxe

from log_time import measure_time

load_dotenv(encoding="utf-8") 
DB_HOST = os.getenv('GP_HOST')
DB_USER = os.getenv('GP_USER')
DB_PASS = os.getenv('GP_PASSWORD')
DB_NAME = os.getenv('GP_DB')
DB_PORT = os.getenv('GP_PORT')
DB_SCHEMA = 'test'
DB_TABLE = 'flight_data'


@measure_time
def get_table_batch(table_name):
    connection_string=(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        f"?client_encoding=utf8")

    context = gx.get_context()

    data_source = context.data_sources.add_postgres(
        "Flight", connection_string=connection_string
    )

    asset_name = "Flight data table"
    database_table_name = table_name
    data_asset = data_source.add_table_asset(
        schema_name=DB_SCHEMA, table_name=database_table_name, name=asset_name
    )

    # Получаем батч (набор данных)
    batch_definition = data_asset.add_batch_definition_whole_table("My batch")
    batch = batch_definition.get_batch()

    return batch


# Count flight data
@measure_time
def flight_data__count(batch):
    expectation_count = gxe.ExpectTableRowCountToBeBetween(min_value=2, max_value=9000000)
    res = batch.validate(expectation_count)

    return res


# Min, Max search date
@measure_time
def flight_data__date_range(batch):
    expectation_date_range = gxe.ExpectColumnValuesToBeBetween(
        column="searchdate", 
        min_value=datetime(2022, 4, 16),
        max_value=datetime(2022, 5, 4)
        )
    res = batch.validate(expectation_date_range)

    return res 


# Count dublicate
@measure_time
def flight_data__dublicate(batch):
    expectation_dbl = gxe.ExpectCompoundColumnsToBeUnique(
        column_list=["legid", "searchdate"]
    )
    res = batch.validate(expectation_dbl)
    
    return res


# Is null flight data
@measure_time
def flight_data__is_null(batch):
    columns = ["legid", "searchdate", "flightdate", "startingairport", "destinationairport", "farebasiscode", "travelduration", "elapseddays", "isbasiceconomy", "isrefundable", "isnonstop", "basefare", "totalfare", "seatsremaining", "totaltraveldistance", "segmentsdeparturetimeepochseconds", "segmentsdeparturetimeraw", "segmentsarrivaltimeepochseconds", "segmentsarrivaltimeraw", "segmentsarrivalairportcode", "segmentsdepartureairportcode", "segmentsairlinename", "segmentsairlinecode", "segmentsequipmentdescription", "segmentsdurationinseconds", "segmentsdistance", "segmentscabincode"]
    print('Is null flight data:')
    n = 0

    for col in columns:
        print(f'{col}:')
        expectation_null = gxe.ExpectColumnValuesToNotBeNull(column=col)
        res = batch.validate(expectation_null)
        if res.success == False:
            print(f"{col} is null: {res.result["unexpected_count"]}")
            n += res.result["unexpected_count"]

    return n


@measure_time
def main():
    batch = get_table_batch(DB_TABLE)

    res = flight_data__count(batch)
    print(f'''
            Count flight data: {res.result["observed_value"]}
            Success: {res.success}
        ''')

    res = flight_data__date_range(batch)
    if res.success:
        print(f'''
                Min, Max search date:
                Success: {res.success}
            ''')
    else:
        print(f"Error search date: {res.result["partial_unexpected_counts"]}")

    res = flight_data__dublicate(batch)
    if res.success:
        print('''Count dublicate: 
            0 dublicate''')
    else:
        print(f"Count dublicate: {res.result}")

    n = flight_data__is_null(batch)
    print(f"Is null flight data: {n}")


if __name__ == "__main__":
    main()