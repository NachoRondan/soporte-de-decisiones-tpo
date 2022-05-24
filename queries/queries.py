import pandas as pd
import pyodbc
from google.cloud import bigquery

def get_excel_db(excel_name):
    df = pd.read_excel('.\\bussiness_dbs\\' + excel_name, index_col=None)  
    return df

def get_access_db(table):
    access_db = ".\\bussiness_dbs\course offerings.accdb"
    conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + access_db + ';')
    query = "SELECT * FROM [" + table + "]"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_bq_table(table_name):
    bqclient = bigquery.Client()
    query_string = "SELECT * FROM `soporte-decisiones-tpo.models." + table_name + "`"

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return df

def get_bq_query(query):
    bqclient = bigquery.Client()
    query_string = query

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return df