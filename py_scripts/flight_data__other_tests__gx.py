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
DB_SCHEMA = 'raw'
DB_TABLE = 'raw_flight_data'


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


@measure_time
def test_flight_data(batch):
    
    # Тест 1: legid не должен содержать NULL
    expectation = gxe.ExpectColumnValuesToNotBeNull(column='legid')
    assert batch.validate(expectation).success, "NULL values found in legid"

    # Тест 2: flightdate >= searchdate
    expectation = gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A='flightdate', column_B='searchdate')
    assert batch.validate(expectation).success, "flightdate < searchdate found"
    
    # Тест 3: startingairport != destinationairport
    expectation = gxe.ExpectColumnPairValuesToBeEqual(column_A='startingairport', column_B='destinationairport')
    assert not batch.validate(expectation).success, "startingairport equals destinationairport"
    
    # Тест 4: totalfare >= basefare
    expectation = gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A='totalfare', column_B='basefare')
    assert batch.validate(expectation).success, "totalfare < basefare found"


@measure_time
def main():
    batch = get_table_batch(DB_TABLE)
    test_flight_data(batch)


if __name__ == "__main__":
    main()