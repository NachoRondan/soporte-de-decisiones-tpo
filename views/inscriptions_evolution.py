import pandas as pd
import sys
sys.path.append('../queries')

from queries import get_bq_query
from google.cloud import bigquery
import warnings
warnings.filterwarnings("ignore")


query = """
        SELECT  s.season as season,
                s.year as Year,
                (s.male_enrolled + s.female_enrolled) as Inscriptions,
                i.instructor_lastname as Instructor,
                c.course as Course,
                rt.description as RoomType,
                s.time as Time,

        FROM `soporte-decisiones-tpo.models.facts_courses_by_season` as s
          left join `soporte-decisiones-tpo.models.dim_instructors` as i on i.instructor_id = s.instructor_id
          left join `soporte-decisiones-tpo.models.dim_courses` as c on c.course_id = s.course_id
          left join `soporte-decisiones-tpo.models.dim_rooms` as r on r.room_id = s.room_id
          left join `soporte-decisiones-tpo.models.dim_room_types` as rt on rt.room_type_id = r.room_type_id """

df =  get_bq_query(query)
df['Year'] = df['Year'].astype(str)
df['Season'] = df['season'] + ' ' + df['Year']
df = df.drop(columns=['season','Year'])

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Inscriptions", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("Season", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Course", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Instructor", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("RoomType", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.views.view_inscriptions_evolution'
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  
job.result()