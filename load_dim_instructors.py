from google.cloud import bigquery
import pandas as pd
import sys
sys.path.append('../queries')
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

dim_instructors = dim_instructors.drop(columns=['zip_code','state','city'])
dim_instructors = dim_instructors.join(dim_address.set_index('street'),on='street')
dim_instructors = dim_instructors.drop(columns=['zip_code','street','city_id','state_id'])
dim_instructors = dim_instructors.rename(columns={'Instructor_Nu':'instructor_id','Instructor':'instructor_lastname','Hiring_date':'hiring_date','Born_date':'born_year','Family_Nu':'family_number','Children_Nu':'children_number','Commission':'last_commision','CommissionRate':'commission_rate','address_id':'primary_address_id'})
dim_instructors['instructor_id'] = dim_instructors.instructor_id.astype('Int64')


#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("instructor_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("hiring_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("born_year", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("instructor_lastname", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("family_number", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("children_number", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("last_commision", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("commission_rate", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("primary_address_id", bigquery.enums.SqlTypeNames.INTEGER),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_instructors'
job = client.load_table_from_dataframe(
    dim_instructors, table_id, job_config=job_config
)  
job.result()