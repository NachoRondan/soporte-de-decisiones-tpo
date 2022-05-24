from google.cloud import bigquery
import pandas as pd
import datetime
from queries import queries as q
import warnings
warnings.filterwarnings("ignore")

fall_2019 = q.get_access_db("Fall 2019")
fall_2019 = fall_2019.rename(columns={'Fall 2001':'course_id'})
fall_2019['year'] = 2019
fall_2019['season'] = 'Fall'

fall_2020 = q.get_access_db("Fall 2020")
fall_2020 = fall_2020.rename(columns={'Fall 2002':'course_id'})
fall_2020['year'] = 2020
fall_2020['season'] = 'Fall'

fall_2021 = q.get_access_db("Fall 2021")
fall_2021 = fall_2021.rename(columns={'Fall 2003':'course_id'})
fall_2021['year'] = 2021
fall_2021['season'] = 'Fall'

spring_2019 = q.get_access_db("Spring 2019")
spring_2019 = spring_2019.rename(columns={'Spring2002':'course_id'})
spring_2019['year'] = 2019
spring_2019['season'] = 'Spring'

spring_2020 = q.get_access_db("Spring 2020")
spring_2020 = spring_2020.rename(columns={'SPRING 2003':'course_id'})
spring_2020['year'] = 2020
spring_2020['season'] = 'Spring'

summer_2019 = q.get_access_db("Summer 2019")
summer_2019 = summer_2019.rename(columns={'Summer 2002':'course_id'})
summer_2019['year'] = 2019
summer_2019['season'] = 'Summer'

summer_2020 = q.get_access_db("Summer 2020")
summer_2020 = summer_2020.rename(columns={'SUMMER 2003':'course_id'})
summer_2020['year'] = 2020
summer_2020['season'] = 'Summer'

seasons = [fall_2020,fall_2021,spring_2019,spring_2020,summer_2019,summer_2020]
facts_courses_by_season = fall_2019

for season in seasons:
  facts_courses_by_season = facts_courses_by_season.append(season)

facts_courses_by_season['Hours'] = pd.to_datetime(facts_courses_by_season['Hours'])
facts_courses_by_season['time'] = [datetime.datetime.time(d) for d in facts_courses_by_season['Hours']]

dim_instructors = q.get_excel_db("Instructors.xlsx")
dim_instructors = dim_instructors.drop(columns=['Street','City','State','ZipCode','Hiring_date','Born_date','Family_Nu','Children_Nu','Commission','CommissionRate'])
dim_instructors = dim_instructors.rename(columns={'Instructor_Nu':'instructor_id','Instructor':'instructor'})
dim_instructors['instructor_id'] = dim_instructors.instructor_id.astype('Int64')

facts_courses_by_season = facts_courses_by_season.rename(columns={'Instructor':'instructor'})
facts_courses_by_season = facts_courses_by_season.join(dim_instructors.set_index('instructor'), on='instructor')



print(facts_courses_by_season.head())