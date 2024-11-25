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

        # Menyusun query untuk mengambil data dari SalesOrderHeader
        query_header = """
        SELECT 
            CAST(OrderDate AS DATE) AS OrderDateKey,
            CAST(DueDate AS DATE) AS DueDateKey,
            CustomerID AS CustomerKey,
            TerritoryID AS SalesTerritoryKey,
            SalesOrderID
        FROM Sales.SalesOrderHeader
        """

        # Mengambil data dari SalesOrderHeader
        df_header = pd.read_sql_query(query_header, engine_sql_server)

        # Menyusun query untuk mengambil data dari SalesOrderDetail
        query_detail = """
        SELECT 
            OrderQty AS OrderQuantity,
            ProductID AS ProductKey,
            UnitPrice,
            UnitPriceDiscount,
            LineTotal AS TotalSales,
            SalesOrderID
        FROM Sales.SalesOrderDetail
        """

        # Mengambil data dari SalesOrderDetail
        df_detail = pd.read_sql_query(query_detail, engine_sql_server)

        # Melakukan join antara df_header dan df_detail berdasarkan SalesOrderID
        df_joined = pd.merge(df_header, df_detail, on='SalesOrderID')

        # Memuat data yang telah digabungkan ke PostgreSQL
        load(df_joined, 'FactSales')

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
