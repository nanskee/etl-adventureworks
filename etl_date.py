from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

# Variabel lingkungan
pwd = os.environ['PGPASS']
uid = os.environ['PGIUD']

sql_server_driver = "{SQL Server Native Client 11.0}"
sql_server = "DESKTOP-HK27CB8\\SQLEXPRESS"
sql_database = "AdventureWorks2019"

pg_host = "localhost"  # Ganti dengan host PostgreSQL Anda
pg_port = "5432"
pg_database = "adventureworks"

def extract():
    """Fungsi untuk mengekstrak data dari SQL Server."""
    try:
        src_conn = pyodbc.connect(
            f'DRIVER={sql_server_driver};SERVER={sql_server};DATABASE={sql_database};UID={uid};PWD={pwd}'
        )
        # Menggunakan SQLAlchemy untuk koneksi ke SQL Server
        engine_sql_server = create_engine(f'mssql+pyodbc://{uid}:{pwd}@{sql_server}/{sql_database}?driver=SQL+Server+Native+Client+11.0')

        # Menghasilkan tanggal dari 20050101 sampai 20141231
        date_range = pd.date_range(start='2005-01-01', end='2014-12-31')

        # Membuat DataFrame dari tanggal
        df = pd.DataFrame(date_range, columns=['FullDateAlternateKey'])
        df['DateKey'] = df['FullDateAlternateKey'].dt.date  # Mengubah DateKey menjadi format date
        df['DayNumberofWeek'] = df['FullDateAlternateKey'].dt.dayofweek + 1
        df['DayNameofWeek'] = df['FullDateAlternateKey'].dt.day_name()
        df['DayNumberofMonth'] = df['FullDateAlternateKey'].dt.day
        df['DayNumberofYear'] = df['FullDateAlternateKey'].dt.dayofyear
        df['WeekNumberofYear'] = df['FullDateAlternateKey'].dt.isocalendar().week
        df['MonthName'] = df['FullDateAlternateKey'].dt.month_name()
        df['MonthNumberofYear'] = df['FullDateAlternateKey'].dt.month
        df['CalendarQuarter'] = df['FullDateAlternateKey'].dt.quarter
        df['CalendarYear'] = df['FullDateAlternateKey'].dt.year
        df['CalendarSemester'] = ((df['CalendarQuarter'] - 1) // 2) + 1

        # Memuat data ke PostgreSQL
        load(df, 'DimDate')

    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

def load(df, tbl):
    """Fungsi untuk memuat data ke PostgreSQL."""
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{pg_host}:{pg_port}/{pg_database}')
        print(f'Importing rows {rows_imported} to {rows_imported + len(df)} for table {tbl}')
        df.to_sql(tbl, engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))

# Memanggil fungsi extract untuk memulai proses ETL
try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
