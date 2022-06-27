from google.cloud import bigquery
import pandas as pd
from queries import queries as q
import warnings
warnings.filterwarnings("ignore")

room_types = q.get_excel_db("Room Types.xlsx")
room_types = room_types.drop(columns=['ROOM','BUILDING'])
room_types = room_types.drop_duplicates()
room_types = room_types.rename(columns={'TYPE':'room_type_code'})
room_types.insert(0, 'room_type_id', range(1,1+len(room_types)))


#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        
        bigquery.SchemaField("room_type_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("room_type_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("description", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_room_types'
job = client.load_table_from_dataframe(
    room_types, table_id, job_config=job_config
)  
job.result()