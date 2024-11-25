from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os
import xml.etree.ElementTree as ET

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

        # Mengambil data dari tabel Customer dengan JOIN
        query = """
            SELECT 
                pp.BusinessEntityID,
                pp.PersonType,
                pp.FirstName,
                pp.MiddleName,
                pp.LastName,
                sc.CustomerID,
                sc.TerritoryID,
                sc.AccountNumber,
                pp.Demographics
            FROM Person.Person pp
            INNER JOIN Sales.Customer sc ON pp.BusinessEntityID = sc.PersonID
            WHERE pp.PersonType = 'IN'
        """
        df = pd.read_sql(query, engine_sql_server)

        # Cek isi dari kolom Demographics
        print("Data Demographics:")
        print(df['Demographics'].head())  # Menampilkan beberapa baris pertama dari kolom Demographics

        # Memproses kolom Demographics
        demographics_data = df['Demographics'].apply(parse_demographics)
        demographics_df = pd.json_normalize(demographics_data)

        # Menggabungkan data Customer dengan Demographics
        final_df = pd.concat([df[['BusinessEntityID', 'PersonType', 'FirstName', 'MiddleName', 'LastName', 'CustomerID', 'TerritoryID', 'AccountNumber']], demographics_df], axis=1)

        return final_df  # Mengembalikan DataFrame untuk transformasi selanjutnya

    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

def transformation(df):
    """Fungsi untuk menggabungkan nama depan, nama tengah, dan nama belakang menjadi FullName."""
    df['FullName'] = df['FirstName'] + ' ' + df['MiddleName'].fillna('') + ' ' + df['LastName']
    df['FullName'] = df['FullName'].str.strip()  # Menghapus spasi ekstra
    
    # Menghapus kolom yang tidak diperlukan
    df.drop(columns=['BusinessEntityID', 'PersonType', 'FirstName', 'MiddleName', 'LastName'], inplace=True)

    # Mengatur ulang urutan kolom
    df = df[['CustomerID', 'FullName', 'TerritoryID', 'AccountNumber'] + 
              [col for col in df.columns if col not in ['CustomerID', 'FullName', 'TerritoryID', 'AccountNumber']]]

    return df

def parse_demographics(demographics_xml):
    """Fungsi untuk mengekstrak informasi dari kolom Demographics yang berformat XML."""
    try:
        # Menghapus namespace untuk mempermudah parsing
        root = ET.fromstring(demographics_xml)
        # Menggunakan namespace yang sesuai
        ns = {'ns': 'http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey'}
        
        return {
            'BirthDate': root.find('ns:BirthDate', ns).text if root.find('ns:BirthDate', ns) is not None else None,
            'MaritalStatus': root.find('ns:MaritalStatus', ns).text if root.find('ns:MaritalStatus', ns) is not None else None,
            'Gender': root.find('ns:Gender', ns).text if root.find('ns:Gender', ns) is not None else None,
            'YearlyIncome': root.find('ns:YearlyIncome', ns).text if root.find('ns:YearlyIncome', ns) is not None else None,
            'TotalChildren': root.find('ns:TotalChildren', ns).text if root.find('ns:TotalChildren', ns) is not None else None,
            'Education': root.find('ns:Education', ns).text if root.find('ns:Education', ns) is not None else None,
            'Occupation': root.find('ns:Occupation', ns).text if root.find('ns:Occupation', ns) is not None else None,
            'HomeOwnerFlag': root.find('ns:HomeOwnerFlag', ns).text if root.find('ns:HomeOwnerFlag', ns) is not None else None,
            'NumberCarsOwned': root.find('ns:NumberCarsOwned', ns).text if root.find('ns:NumberCarsOwned', ns) is not None else None,
            'CommuteDistance': root.find('ns:CommuteDistance', ns).text if root.find('ns:CommuteDistance', ns) is not None else None,
        }
    except Exception as e:
        print(f"Error parsing demographics XML: {e}")

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
    df = extract()
    df = transformation(df)  # Melakukan transformasi setelah ekstraksi
    load(df, 'DimCustomer')   # Memuat data setelah transformasi
except Exception as e:
    print("Error while processing data: " + str(e))
