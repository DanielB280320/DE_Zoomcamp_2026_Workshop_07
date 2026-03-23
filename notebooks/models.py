import pandas as pd
import json
from dataclasses import dataclass
import dataclasses
from kafka import KafkaProducer

@dataclass   
class ride: 
    lpep_pickup_datetime: int
    lpep_dropoff_datetime: int
    PULocationID: int
    DOLocationID: int
    passenger_count: int 
    trip_distance: float
    tip_amount: float
    total_amount: float

def df_row_to_ride(row):
    return ride(
        lpep_pickup_datetime= int(row.lpep_pickup_datetime.timestamp() *1000),
        lpep_dropoff_datetime= int(row.lpep_dropoff_datetime.timestamp() *1000),
        PULocationID= int(row.PULocationID),
        DOLocationID= int(row.DOLocationID),
        passenger_count= int(row.passenger_count),
        trip_distance= float(row.trip_distance),
        tip_amount= float(row.tip_amount),
        total_amount= float(row.total_amount)
    )

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')