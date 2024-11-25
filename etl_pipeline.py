from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

pwd = os.environ['PGPASS']
uid = os.environ['PGIUD']

sql_server_driver = "{SQL Server Native Client 11.0}"
sql_server = "DESKTOP-HK27CB8\\SQLEXPRESS"
sql_database = "AdventureWorks2019"

pg_host =   "localhost"
pg_port = "5432"
pg_database = "adventureworks"

def extract():
    try:
        src_conn = pyodbc.connect(
            f'DRIVER={sql_server_driver};SERVER={sql_server};DATABASE={sql_database};UID={uid};PWD={pwd}'
        )
        src_cursor = src_conn.cursor()

        src_cursor.execute(""" 
        SELECT t.name as table_name
        FROM sys.tables t 
        WHERE t.name IN ('DimCurrency', 'DimCustomer', 'DimDate', 'DimEmployee', 'FactInternetSales', 'FactResellerSales')
        """)
        src_tables = src_cursor.fetchall()
        
        for tbl in src_tables:
            df = pd.read_sql_query(f'SELECT * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{pg_host}:{pg_port}/{pg_database}')
        print(f'Importing rows {rows_imported} to {rows_imported + len(df)} for table {tbl}')
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
