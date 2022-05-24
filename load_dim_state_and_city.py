from email.utils import localtime
from google.cloud import bigquery
import pandas as pd
from queries import queries as q
import warnings
warnings.filterwarnings("ignore")

dim_instructors = q.get_excel_db("Instructors.xlsx")
dim_instructors = dim_instructors.rename(columns={'Street':'street','City':'city','State':"state", 'ZipCode':'zip_code'})


location = dim_instructors[['street','city','state', 'zip_code']]


dim_state = location[['state']].drop_duplicates()
dim_state.insert(0, 'state_id', range(1,1+len(dim_state)))

dim_city = location[['city','state']].drop_duplicates(subset='state',keep='first')
dim_city.insert(0, 'city_id', range(1,1+len(dim_city)))
dim_city = dim_city.join(dim_state.set_index('state'), on='state')
dim_city = dim_city.drop(columns=['state'])

dim_address = location.join(dim_city.set_index('city'), on='city')
dim_address = dim_address.drop(columns=['state','city'])
dim_address.insert(0, 'address_id', range(1,1+len(dim_address)))


#Upload address to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("address_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("zip_code", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("city_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("state_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("street", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_address'
job = client.load_table_from_dataframe(
    dim_address, table_id, job_config=job_config
)  
job.result()


#Upload city to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("city_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("city", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct state a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_city'
job = client.load_table_from_dataframe(
    dim_city, table_id, job_config=job_config
)  
job.result()

#Upload city to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("state_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("state", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_state'
job = client.load_table_from_dataframe(
    dim_state, table_id, job_config=job_config
)  
job.result()