from log_time import measure_time

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_SCHEMA = 'test'

def init_db_connection():
    DB_HOST = os.getenv('GP_HOST')
    DB_USER = os.getenv('GP_USER')
    DB_PASS = os.getenv('GP_PASSWORD')
    DB_NAME = os.getenv('GP_DB')
    DB_PORT = os.getenv('GP_PORT')

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}', client_encoding='utf8')

    conn = engine.connect()

    return conn


@measure_time
def flight_data__count(conn, DB_SCHEMA):
    result = conn.execute(text(f"SELECT count(1) FROM {DB_SCHEMA}.flight_data;"))
    count = result.fetchone()

    return count


@measure_time
def flight_data__date_range(conn, DB_SCHEMA):
    result = conn.execute(text(f"SELECT  MIN(searchdate) AS min_search_date, MAX(searchdate) AS max_search_date FROM {DB_SCHEMA}.flight_data;"))
    row = result.fetchone()
    
    return row


@measure_time
def flight_data__dublicate(conn, DB_SCHEMA):
    result = conn.execute(text(f"""
    select coalesce(sum(cnt), count(1)) from 
    ( select
            count(1) as cnt
        from {DB_SCHEMA}.flight_data
        group by legid, searchdate
        having count(1) > 1 ) t;"""))

    row = result.fetchone()

    return row


@measure_time
def flight_data__is_null(conn, DB_SCHEMA):
    result = conn.execute(text(f"""
    select 
        sum(case 
                when legid is null 
                or searchdate is null
                or flightdate is null
                or startingairport is null
                or destinationairport is null
                or farebasiscode is null
                or travelduration is null
                or elapseddays is null
                or isbasiceconomy is null
                or isrefundable is null
                or isnonstop is null
                or basefare is null
                or totalfare is null
                or seatsremaining is null
                or totaltraveldistance is null
                or segmentsdeparturetimeepochseconds is null
                or segmentsdeparturetimeraw is null
                or segmentsarrivaltimeepochseconds is null
                or segmentsarrivaltimeraw is null
                or segmentsarrivalairportcode is null
                or segmentsdepartureairportcode is null
                or segmentsairlinename is null
                or segmentsairlinecode is null
                or segmentsequipmentdescription is null
                or segmentsdurationinseconds is null
                or segmentsdistance is null
                or segmentscabincode is null
            then 1 else 0 
            end) as is_null
    from {DB_SCHEMA}.flight_data;"""))

    row = result.fetchone()

    return row


@measure_time
def main():
    conn = init_db_connection()

    count = flight_data__count(conn, DB_SCHEMA)
    print(f"Count flight data: {count}")

    row = flight_data__date_range(conn, DB_SCHEMA)
    print(f"Min search date: {row.min_search_date}, Max search date: {row.max_search_date}")

    dublicate_count = flight_data__dublicate(conn, DB_SCHEMA)
    print(f"Count dublicate: {dublicate_count}")
    
    null_counts = flight_data__is_null(conn, DB_SCHEMA)
    print(f"Is null flight data: {null_counts}")

    conn.close()


if __name__ == "__main__":
    main()