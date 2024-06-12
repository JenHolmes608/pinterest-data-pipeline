import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml

random.seed(100)

class AWSDBConnector:

    def __init__(self):
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

kinesis_client = boto3.client('kinesis', region_name='eu-west-1')

def send_to_kinesis(stream_name, data):
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey="partition_key"
    )

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                send_to_kinesis('streaming-0afff69adbe3-pin', pin_result)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                send_to_kinesis('streaming-0afff69adbe3-geo', geo_result)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                send_to_kinesis('streaming-0afff69adbe3-user', user_result)

            print(pin_result)
            print(geo_result)
            print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
