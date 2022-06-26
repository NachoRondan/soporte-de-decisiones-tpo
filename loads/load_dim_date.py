from google.cloud import bigquery
import pandas as pd
import sys
sys.path.append('./queries')
import datetime
from queries import get_access_db,get_excel_db,get_bq_query
import warnings
warnings.filterwarnings("ignore")

dim_date = get_excel_db('dim_date.xlsx')
dim_season = get_excel_db('dim_season.xlsx')

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("date_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("day", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("month", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("season_id", bigquery.enums.SqlTypeNames.INTEGER),

    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_date'
job = client.load_table_from_dataframe(
    dim_date, table_id, job_config=job_config
)  

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("season_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("season", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_season'
job = client.load_table_from_dataframe(
    dim_season, table_id, job_config=job_config
)  