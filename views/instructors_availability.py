import datetime as dt
import pandas as pd
import sys
sys.path.append('../queries')
from queries import get_bq_query
from google.cloud import bigquery
import warnings
warnings.filterwarnings("ignore")


query = """ SELECT i.instructor_lastname as Instructor, d.days as Days, f.year as Year, f.season as Season
FROM `soporte-decisiones-tpo.models.facts_courses_by_season` as f
left join `soporte-decisiones-tpo.models.dim_instructors` as i on i.instructor_id = f.instructor_id
left join `soporte-decisiones-tpo.models.dim_days_combination` as d on d.days_combination_id = f.days_combination_id """

df =  get_bq_query(query)
df['Season'] = df['Season'] + ' ' + df['Year'].astype(str)
df = df.drop(columns=['Year'])
df = df.groupby(['Instructor','Season'])['Days'].apply(lambda x: x.sum()).reset_index()

days = {'Lunes':'M','Martes':'T','Miercoles':'W','Jueves':'R','Viernes':'F'}
def check_availability(row,day):
  if  days[day] in row['Days']:
    return '-'
  return  'Disponible'
  
for day in list(days.keys()):
  df[day] = df.apply(check_availability,args=(day,),axis=1)
df = df.drop(columns=['Days'])

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Instructor", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Season", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.views.view_instructors_availability'
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  
job.result()