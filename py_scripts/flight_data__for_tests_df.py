from log_time import measure_time

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

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
def fetch_flight_data(engine):
    df = pd.read_sql_table(
        table_name='flight_data', 
        con=engine, 
        schema='test',
        columns=['legid', 
                 'searchdate']
        )
    return df


chunk_size = 50000 
total_rows = 0


@measure_time
def flight_data__count(df):
    count = df['legid'].count()

    return count


@measure_time
def flight_data__date_range(df):
    min_search_date = df['searchdate'].min()
    max_search_date = df['searchdate'].max()
    
    return min_search_date, max_search_date


@measure_time
def flight_data__dublicate(df):
    grouped = df.groupby([
        "legid", 
        "searchdate"
    ])
    duplicate_keys = grouped.filter(lambda x: len(x) > 1)

    duplicate_count = duplicate_keys.groupby([
        "legid", 
        "searchdate"
    ]).ngroups

    return duplicate_count


@measure_time
def flight_data__is_null(engine):
    for chunk in pd.read_sql_table(
        'flight_data', 
        engine, 
        schema='test',
        chunksize=chunk_size
    ):
        null_counts = pd.Series()
        chunk_null_counts = chunk.isnull().sum()
        null_counts = null_counts.add(chunk_null_counts, fill_value=0)
        total_rows += len(chunk)

    return null_counts


@measure_time
def main():
    engine, conn = init_db_connection()
    df = fetch_flight_data(engine)

    count = flight_data__count(df)
    print(f"Count flight data: {count}")

    min_search_date, max_search_date = flight_data__date_range(df)
    print(f"Min search date: {min_search_date}")
    print(f"Max search date: {max_search_date}")

    dublicate_count = flight_data__dublicate(df)
    print(f"Count dublicate: {dublicate_count}")
    
    # null_counts = flight_data__is_null(engine)
    # print(f"Is null flight data: {null_counts}")

    conn.close()


if __name__ == "__main__":
    main()