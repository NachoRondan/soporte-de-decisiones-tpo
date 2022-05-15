import collections
from google.cloud import bigquery
import pandas as pd
from queries import queries as q
import warnings
warnings.filterwarnings("ignore")

rooms_dataframe = q.get_access_db("Rooms")

rooms_dataframe = rooms_dataframe.rename(columns={"LOCATION":'building_code', "Location Name":'description'})
rooms_dataframe = rooms_dataframe.drop(columns=['ROOM','SEATS'])
rooms_dataframe = rooms_dataframe.drop_duplicates()
rooms_dataframe.insert(0, 'building_id', range(1,1+len(rooms_dataframe)))


#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("building_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("building_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("description", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_buildings'
job = client.load_table_from_dataframe(
    rooms_dataframe, table_id, job_config=job_config
)  
job.result()