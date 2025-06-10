import os
import pandas as pd
from sqlalchemy import create_engine
from log_time import measure_time

from dotenv import load_dotenv

load_dotenv()
DB_SCHEMA = 'raw'


def init_db_connection():
    DB_HOST = os.getenv('GP_HOST')
    DB_USER = os.getenv('GP_USER')
    DB_PASS = os.getenv('GP_PASSWORD')
    DB_NAME = os.getenv('GP_DB')
    DB_PORT = os.getenv('GP_PORT')

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}', client_encoding='utf8')

    conn = engine.connect()

    return engine, conn


@measure_time
def test_flight_data(engine, DB_SCHEMA):

    df = pd.read_sql_table(
        table_name='raw_flight_data', 
        con=engine, 
        schema=DB_SCHEMA,
        columns=['legid', 'flightdate', 'searchdate', 'startingairport', 'destinationairport', 'basefare', 'totalfare']
    )
    
    # Тест 1: legid не должен содержать NULL
    assert df['legid'].isnull().sum() == 0, "NULL values found in legid"
    
    # Тест 2: flightdate >= searchdate
    assert (df['flightdate'] >= df['searchdate']).all(), "flightdate < searchdate found"
    
    # Тест 3: startingairport != destinationairport
    assert (df['startingairport'] != df['destinationairport']).all(), "startingairport equals destinationairport"
    
    # Тест 4: totalfare >= basefare
    assert (df['totalfare'] >= df['basefare']).all(), "totalfare < basefare found"


@measure_time
def main():
    engine, conn = init_db_connection()

    test_flight_data(engine, DB_SCHEMA)

    conn.close()


if __name__ == "__main__":
    main()