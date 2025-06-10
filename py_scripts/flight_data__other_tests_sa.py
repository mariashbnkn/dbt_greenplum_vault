import os
from sqlalchemy import create_engine, text
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

    return conn


def test_flight_data(conn, DB_SCHEMA):

    # Тест 1: Проверка NULL в legid
    null_legid = conn.execute(text(f"""
        SELECT COUNT(*) FROM {DB_SCHEMA}.raw_flight_data WHERE legid IS NULL
    """)).scalar()
    assert null_legid == 0, f"Found {null_legid} NULL values in legid"
    
    # Тест 2: flightdate < searchdate
    invalid_dates = conn.execute(text(f"""
        SELECT COUNT(*) FROM {DB_SCHEMA}.raw_flight_data 
            WHERE flightdate < searchdate
    """)).scalar()
    assert invalid_dates == 0, f"Found {invalid_dates} records with flightdate < searchdate"
    
    # Тест 3: startingairport = destinationairport
    same_airports = conn.execute(text(f"""
        SELECT COUNT(*) FROM {DB_SCHEMA}.raw_flight_data 
        WHERE startingairport = destinationairport
    """)).scalar()
    assert same_airports == 0, f"Found {same_airports} records with same airports"
    
    # Тест 4: totalfare < basefare
    invalid_fares = conn.execute(text(f"""
        SELECT COUNT(*) FROM {DB_SCHEMA}.raw_flight_data 
        WHERE totalfare < basefare
    """)).scalar()
    assert invalid_fares == 0, f"Found {invalid_fares} records with totalfare < basefare"


@measure_time
def main():
    conn = init_db_connection()
    test_flight_data(conn, DB_SCHEMA)

    conn.close()


if __name__ == "__main__":
    main()