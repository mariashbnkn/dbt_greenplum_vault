import os
import pandas as pd
from sqlalchemy import create_engine
from log_time import measure_time
from dotenv import load_dotenv

load_dotenv()
DB_HOST = os.getenv('GP_HOST')
DB_USER = os.getenv('GP_USER')
DB_PASS = os.getenv('GP_PASSWORD')
DB_NAME = os.getenv('GP_DB')
DB_PORT = os.getenv('GP_PORT')
DB_SCHEMA = 'test'


@measure_time
def flight_data__is_null(chunk):
    chunk_null_counts = chunk.isnull().sum()

    return chunk_null_counts


@measure_time
def main():
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}', client_encoding='utf8')

    null_counts = pd.Series()
    total_rows = 0
    chunk_size = 50000

    for chunk in pd.read_sql_table(
        'flight_data', 
        engine, 
        schema='test',
        chunksize=chunk_size
    ):
        
        chunk_null_counts = flight_data__is_null(chunk)

        null_counts = null_counts.add(chunk_null_counts, fill_value=0)
        total_rows += len(chunk)
        print(f"Обработано {total_rows} строк...")

    # Выводим результаты
    print("\nКоличество пустых значений по столбцам:")
    print(null_counts)
    print(f"\nВсего строк: {total_rows}") 


if __name__ == "__main__":
    main()                                                                                                                    