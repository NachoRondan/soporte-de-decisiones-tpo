import datetime as dt
import pandas as pd
import sys
sys.path.append('../queries')

from queries import get_bq_query
from google.cloud import bigquery
import warnings
warnings.filterwarnings("ignore")


query = """ SELECT  * FROM `soporte-decisiones-tpo.models.dim_instructors` """

df =  get_bq_query(query)
df['Salary'] = df['last_commision'] / df['commission_rate']
df = df.rename(columns={'instructor_lastname':'Instructor'})

#Adding Salary range
def add_salary_range(row):
  if row['Salary'] <= 500:
    range = '< 500'
  elif row['Salary'] <= 1000:
    range = '500 - 1000'
  elif row['Salary'] <= 2000:
    range = '1000 - 2000'
  elif row['Salary'] <= 3000:
    range = '2000 - 3000'
  elif row['Salary'] <= 4000:
    range = '3000 - 4000'
  elif row['Salary'] <= 5000:
    range = '4000 - 5000'
  else:
    range = '>5000'
  return range

df['SalaryRange'] = df.apply(add_salary_range,axis=1)

def add_hiring_range(row):
  if row['TimeSinceHiring'] <= 1:
    range = '< un año'
  elif row['TimeSinceHiring'] <= 5:
    range = '1 - 5'
  elif row['TimeSinceHiring'] <= 10:
    range = '6 - 10'
  else:
    range = '>10'
  return range

df['year'] = df['hiring_date'].dt.year
df['TimeSinceHiring'] = 2022 - df['year']
df['HiringRange'] = df.apply(add_hiring_range,axis=1)

#Adding age range
def add_range(row):
  if row['Age'] <= 30:
    range = '20 - 30'
  elif row['Age'] <= 40:
    range = '31 - 40'
  elif row['Age'] <= 50:
    range = '41 - 50'
  elif row['Age'] <= 60:
    range = '51 - 60'
  else:
    range = '>60'
  return range

df['Age'] = 2022 - df['born_year']
df['AgeRange'] = df.apply(add_range,axis=1)

df_columns = list(df.columns.values)
df_columns.remove('AgeRange')
df_columns.remove('HiringRange')
df_columns.remove('SalaryRange')
df_columns.remove('Instructor')

for column in df_columns:
  df = df.drop(columns=[column])

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Instructor", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("SalaryRange", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("AgeRange", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("HiringRange", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.views.view_instructors'
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  
job.result()