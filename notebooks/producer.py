#!/usr/bin/env python
# coding: utf-8

# In[40]:


import pandas as pd
import numpy as np
from dataclasses import dataclass
import dataclasses
import json
from kafka import KafkaProducer


# In[41]:


url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet'


# In[42]:


select_columns = ['lpep_pickup_datetime', 
                  'lpep_dropoff_datetime',
                  'PULocationID', 
                  'DOLocationID',
                  'passenger_count', 
                  'trip_distance', 
                  'tip_amount', 
                  'total_amount'
                  ]

df_green = pd.read_parquet(
    url, 
    columns= select_columns
    )

df_green.head(2)


# In[43]:


# Handling missing values
df_green = df_green.replace(np.nan, 0)


# In[44]:


df_green.info()


# In[45]:


row = df_green.iloc[0, :]
row


# In[46]:


@dataclass   
class ride: 
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: int 
    trip_distance: float
    tip_amount: float
    total_amount: float


# In[47]:


def df_row_to_ride(row):
    return ride(
        lpep_pickup_datetime= str(row.lpep_pickup_datetime),
        lpep_dropoff_datetime= str(row.lpep_dropoff_datetime),
        PULocationID= int(row.PULocationID),
        DOLocationID= int(row.DOLocationID),
        passenger_count= int(row.passenger_count),
        trip_distance= float(row.trip_distance),
        tip_amount= float(row.tip_amount),
        total_amount= float(row.total_amount)
    )


# In[48]:


trip = df_row_to_ride(df_green.iloc[0, :])
trip


# In[49]:


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers= [server],
    value_serializer= ride_serializer
)


# In[50]:


import time

topic_name= 'green-trips'
t0 = time.time()

for _, row in df_green.iterrows():
    trip = df_row_to_ride(row)
    producer.send(topic_name, value=trip)
    #print(f"Sent: {trip}")
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')


# In[ ]:





# In[ ]:




