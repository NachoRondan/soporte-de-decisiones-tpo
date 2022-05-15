from google.cloud import bigquery
import pandas as pd
import numpy as np
from queries import queries as q
import warnings
warnings.filterwarnings("ignore")

# Query access db
rooms = q.get_access_db("Rooms")
rooms = rooms.rename(columns={'ROOM':'room_code','SEATS':'capacity','LOCATION':'building_code',"Location Name":'building'})
rooms['merged_key'] = rooms['room_code'] + rooms['building_code']

# Query rooms excel
room_types = q.get_excel_db("Room Types.xlsx")
room_types['ROOM'] = room_types['ROOM'].astype(str)


# Cretae merged key for room type
conditions = [
  room_types['BUILDING'].eq('Adams Hall'),
  room_types['BUILDING'].eq('Bizzell Library'),
  room_types['BUILDING'].eq('Copeland Hall'),
  room_types['BUILDING'].eq('George Lynn Cross Hall'),
  room_types['BUILDING'].eq('Nichols Center'),
  room_types['BUILDING'].eq('Physical Sciences Center'),
]
choices = [
  'AH',
  'BL',
  'CH',
  'GLC',
  'OKC',
  'PHSC'
]

room_types['building_code'] = np.select(conditions, choices, default="")
room_types['merged_key'] = room_types['ROOM'] + room_types['building_code']

# Add room_type_id
conditions = [
    room_types['TYPE'].eq('t/b'),
    room_types['TYPE'].eq('t/c'),
    room_types['TYPE'].eq('t/a'),
]
choices = [1,2,3]
room_types['room_type_id'] = np.select(conditions, choices, default="")
room_types = room_types.drop(columns=['description','BUILDING','ROOM','building_code'])

# Join room types and rooms
rooms = rooms.join(room_types.set_index('merged_key'), on='merged_key')

# Add building_id
conditions = [
    rooms['building_code'].eq('AH'),
    rooms['building_code'].eq('BL'),
    rooms['building_code'].eq('CH'),
    rooms['building_code'].eq('GLC'),
    rooms['building_code'].eq('OKC'),
    rooms['building_code'].eq('PHSC'),
]
choices = [1,2,3,4,5,6]
rooms['building_id'] = np.select(conditions, choices, default="")

# Clean up
rooms = rooms.drop_duplicates(subset='merged_key', keep='first')
rooms = rooms.drop(columns=['building_code','building','merged_key','TYPE'])
rooms.insert(0, 'room_id', range(1,1+len(rooms)))
rooms['room_type_id'] = rooms['room_type_id'].astype('Int64')
rooms['building_id'] = rooms['room_type_id'].astype('Int64')

print(rooms.info())

#Upload to Big Guery
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("room_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("room_type_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("building_id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("room_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("capacity", bigquery.enums.SqlTypeNames.INTEGER),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'soporte-decisiones-tpo.models.dim_rooms'
job = client.load_table_from_dataframe(
    rooms, table_id, job_config=job_config
)  
job.result()