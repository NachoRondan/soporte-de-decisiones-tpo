from ast import arg
import datetime as dt
from typing import List
import pandas as pd
import sys
sys.path.append('../queries')

from queries import get_bq_query
from google.cloud import bigquery
import warnings
warnings.filterwarnings("ignore")

query = """ SELECT(f.course_season_id) as CourseId,
      c.course as Course,
      f.season as season, 
      f.year as Year,
      (f.male_enrolled + f.female_enrolled) as Inscriptions,
      d.days as Days
FROM `soporte-decisiones-tpo.models.facts_courses_by_season` as f
left join `soporte-decisiones-tpo.models.dim_courses` as c on c.course_id = f.course_id
left join `soporte-decisiones-tpo.models.dim_days_combination` as d on d.days_combination_id = f.days_combination_id"""

df =  get_bq_query(query)

def add_course_duration(row):
  value = len(row['Days'])
  if 'R' in row['Days']:
    value += 1
  return str(value * 10) + ' hs'

df['CourseDuration'] = df.apply(add_course_duration,axis=1)
df['Season'] = df['season'] + " " + df['Year'].astype(str)
df = df.drop(columns=['Days','CourseId','Year','season','Course'])


#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Season", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("CourseDuration", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Inscriptions", bigquery.enums.SqlTypeNames.INTEGER),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.views.view_course_duration'
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  
job.result()