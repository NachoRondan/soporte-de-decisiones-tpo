from google.cloud import bigquery
import pandas as pd
import sys
sys.path.append('./queries')
import datetime
from queries import get_access_db,get_excel_db,get_bq_query
import warnings
warnings.filterwarnings("ignore")

fall_2019 = get_access_db("Fall 2019")
fall_2019 = fall_2019.rename(columns={'Fall 2001':'course_season_id'})
fall_2019['year'] = 2019
fall_2019['season'] = 'Fall'

fall_2020 = get_access_db("Fall 2020")
fall_2020 = fall_2020.rename(columns={'Fall 2002':'course_season_id'})
fall_2020['year'] = 2020
fall_2020['season'] = 'Fall'

fall_2021 = get_access_db("Fall 2021")
fall_2021 = fall_2021.rename(columns={'Fall 2003':'course_season_id'})
fall_2021['year'] = 2021
fall_2021['season'] = 'Fall'

spring_2019 = get_access_db("Spring 2019")
spring_2019 = spring_2019.rename(columns={'Spring2002':'course_season_id'})
spring_2019['year'] = 2019
spring_2019['season'] = 'Spring'

spring_2020 = get_access_db("Spring 2020")
spring_2020 = spring_2020.rename(columns={'SPRING 2003':'course_season_id'})
spring_2020['year'] = 2020
spring_2020['season'] = 'Spring'

summer_2019 = get_access_db("Summer 2019")
summer_2019 = summer_2019.rename(columns={'Summer 2002':'course_season_id'})
summer_2019['year'] = 2019
summer_2019['season'] = 'Summer'

summer_2020 = get_access_db("Summer 2020")
summer_2020 = summer_2020.rename(columns={'SUMMER 2003':'course_season_id'})
summer_2020['year'] = 2020
summer_2020['season'] = 'Summer'

seasons = [fall_2020,fall_2021,spring_2019,spring_2020,summer_2019,summer_2020]
facts_courses_by_season = fall_2019

for season in seasons:
  facts_courses_by_season = facts_courses_by_season.append(season)

facts_courses_by_season['Hours'] = pd.to_datetime(facts_courses_by_season['Hours'])
facts_courses_by_season['time'] = [datetime.datetime.time(d) for d in facts_courses_by_season['Hours']]

#Instructors join
dim_instructors = get_excel_db("Instructors.xlsx")
dim_instructors = dim_instructors.drop(columns=['Street','City','State','ZipCode','Hiring_date','Born_date','Family_Nu','Children_Nu','Commission','CommissionRate'])
dim_instructors = dim_instructors.rename(columns={'Instructor_Nu':'instructor_id','Instructor':'instructor'})
dim_instructors['instructor_id'] = dim_instructors.instructor_id.astype('Int64')

facts_courses_by_season = facts_courses_by_season.rename(columns={'Instructor':'instructor'})
facts_courses_by_season = facts_courses_by_season.join(dim_instructors.set_index('instructor'), on='instructor')

# Courses dim and join
dim_courses = facts_courses_by_season[{'Course Title'}]
dim_courses = dim_courses.drop_duplicates(subset='Course Title',keep='first')
dim_courses.insert(0, 'course_id', range(1,1+len(dim_courses)))

facts_courses_by_season = facts_courses_by_season.join(dim_courses.set_index('Course Title'), on='Course Title')
dim_courses = dim_courses.rename(columns={'Course Title':'course'})

# Days combinations dim and join
dim_days_combination = facts_courses_by_season[['Days']]
dim_days_combination = dim_days_combination.drop_duplicates(subset='Days', keep='first')
dim_days_combination.insert(0, 'days_combination_id', range(1,1+len(dim_days_combination)))
facts_courses_by_season = facts_courses_by_season.join(dim_days_combination.set_index('Days'),on='Days')
dim_days_combination = dim_days_combination.rename(columns={'Days':'days'})
def is_remote(row):
    if('r' in row['Days'].lower()):
        return 1
    return 0

facts_courses_by_season['is_remote'] = facts_courses_by_season.apply(is_remote,axis=1)

# Room Id join
query = """SELECT * 
FROM `soporte-decisiones-tpo.models.dim_rooms` as r
left join `soporte-decisiones-tpo.models.dim_buildings` as b on r.building_id = b.building_id
"""

dim_rooms = get_bq_query(query)
dim_rooms['Room'] = dim_rooms['building_code'] + " " + dim_rooms['room_code']
dim_rooms = dim_rooms.drop(columns=['room_code','capacity','room_type_id','building_id','building_id_1','building_code','description'])
dim_rooms = dim_rooms.dropna()
dim_rooms = dim_rooms.drop_duplicates(subset='Room')
facts_courses_by_season = facts_courses_by_season.join(dim_rooms.set_index('Room'),on='Room')

dim_capacity = get_excel_db('CourseCapacity.xlsx')
dim_capacity['key'] = dim_capacity['Course'] + dim_capacity['Season']
dim_capacity = dim_capacity.drop(columns=['Course Title','Season'])
facts_courses_by_season['key'] = facts_courses_by_season['course_season_id'] + facts_courses_by_season['season'] + " " + facts_courses_by_season['year'].astype(str)
facts_courses_by_season = facts_courses_by_season.join(dim_capacity.set_index('key'),on='key')

facts_courses_by_season = facts_courses_by_season.drop_duplicates(subset=['key'],keep='first')


# Last cleaning
facts_courses_by_season = facts_courses_by_season.drop(columns=['Course Title','Hours','instructor','Days','Room','key','Course'])
facts_courses_by_season = facts_courses_by_season.rename(columns={'Male Enrolled':'male_enrolled','Female Enrolled':'female_enrolled','Size':'expected_capacity'})
facts_courses_by_season['male_enrolled'] = facts_courses_by_season.male_enrolled.astype('Int64')
facts_courses_by_season['female_enrolled'] = facts_courses_by_season.female_enrolled.astype('Int64')
facts_courses_by_season['room_id'] = facts_courses_by_season.room_id.astype('Int64')

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("course_season_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("male_enrolled", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("female_enrolled", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("season", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("time", bigquery.enums.SqlTypeNames.TIME),
        bigquery.SchemaField("instructor_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("course_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("days_combination_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("room_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("is_remote", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("expected_capacity", bigquery.enums.SqlTypeNames.INTEGER),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.facts_courses_by_season'
job = client.load_table_from_dataframe(
    facts_courses_by_season, table_id, job_config=job_config
)  



#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("course", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("course_id", bigquery.enums.SqlTypeNames.INTEGER),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_courses'
job = client.load_table_from_dataframe(
    dim_courses, table_id, job_config=job_config
)  

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("days", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("days_combination_id", bigquery.enums.SqlTypeNames.INTEGER),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_days_combination'
job = client.load_table_from_dataframe(
    dim_days_combination, table_id, job_config=job_config
)  