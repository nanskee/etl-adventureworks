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

        # Menyusun query untuk mengambil data
        query = """
        SELECT 
            return_date AS ReturnDateKey, 
            territory_key AS ReturnTerritoryKey,
            product_key AS ProductKey,
            return_quantity AS ReturnQuantity
        FROM dbo.Returns
        """
        
        # Mengambil data dengan query yang sudah diubah
        df = pd.read_sql_query(query, engine_sql_server)

        # Memanggil fungsi transformasi
        df = transformation(df)

        # Mengurutkan DataFrame berdasarkan ProductCategoryKey
        df.sort_values(by='ReturnDateKey', ascending=True, inplace=True)

        # Memuat data ke PostgreSQL
        load(df, 'FactReturns')

    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

def transformation(df):
    """Fungsi untuk melakukan transformasi pada DataFrame."""
    # Mengubah ReturnDateKey menjadi format 'YYYY-MM-DD' tanpa waktu
    df['ReturnDateKey'] = pd.to_datetime(df['ReturnDateKey']).dt.date

    return df

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
