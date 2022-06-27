import datetime as dt
import pandas as pd
import sys
sys.path.append('../queries')
from queries import get_bq_query
from google.cloud import bigquery
import warnings
warnings.filterwarnings("ignore")


query = """ SELECT f.instructor_id as Instructor,
      max(f.is_remote) as Remote,
      avg(i.commission_rate) as Rate,
      avg(i.last_commision/i.commission_rate) as Salary
FROM `soporte-decisiones-tpo.models.facts_courses_by_season` as f
left join `soporte-decisiones-tpo.models.dim_instructors` as i on i.instructor_id = f.instructor_id
where year = 2021 and season = 'Fall'
group by f.instructor_id"""

df =  get_bq_query(query)


#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Instructor", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("Remote", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("Rate", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("Salary", bigquery.enums.SqlTypeNames.FLOAT),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.views.view_instructors_avg'
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  
job.result()