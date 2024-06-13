import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime

random.seed(100)

class AWSDBConnector:
    """
    A class to manage the connection to the AWS RDS MySQL database.

    Attributes:
    HOST (str): Database host address.
    USER (str): Database username.
    PASSWORD (str): Database password.
    DATABASE (str): Database name.
    PORT (int): Database port number.
    """
    
    def __init__(self):
        """
        Initializes the AWSDBConnector by loading database credentials from a YAML file.
        """
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']
        
    def create_db_connector(self):
        """
        Creates a SQLAlchemy engine for connecting to the MySQL database.

        Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine object.
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def serialize_row(row):
    """
    Serializes datetime objects in a database row to ISO format strings.

    Args:
    row (dict): A dictionary representing a row from the database.

    Returns:
    dict: The modified row dictionary with datetime objects serialized to strings.
    """
    for key, value in row.items():
        if isinstance(value, datetime):
            row[key] = value.isoformat()
    return row

def run_infinite_post_data_loop():
    """
    Continuously fetches data from the database and sends it to specified endpoints.
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_result = serialize_row(pin_result)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result = serialize_row(geo_result)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result = serialize_row(user_result)

            print(pin_result)
            print(geo_result)
            print(user_result)

            pin_payload = json.dumps({
                "StreamName": "streaming-0afff69adbe3-pin",
                "Data": pin_result, 
                "PartitionKey": "partition-1"
            })

            kin_pin_response = requests.request("PUT", 'https://sqixei7ili.execute-api.us-east-1.amazonaws.com/newstage/streams/streaming-0afff69adbe3-pin/record', headers={'Content-Type': 'application/json'}, data=pin_payload)
            print(kin_pin_response.status_code)

            geo_payload = json.dumps({
                "StreamName": "streaming-0afff69adbe3-geo",
                "Data": geo_result, 
                "PartitionKey": "partition-2"
            })

            kin_geo_response = requests.request("PUT", 'https://sqixei7ili.execute-api.us-east-1.amazonaws.com/newstage/streams/streaming-0afff69adbe3-geo/record', headers={'Content-Type': 'application/json'}, data=geo_payload)
            print(kin_geo_response.status_code)

            user_payload = json.dumps({
                "StreamName": "streaming-0afff69adbe3-user",
                "Data": user_result, 
                "PartitionKey": "partition-3"
            })

            kin_user_response = requests.request("PUT", 'https://sqixei7ili.execute-api.us-east-1.amazonaws.com/newstage/streams/streaming-0afff69adbe3-user/record', headers={'Content-Type': 'application/json'}, data=user_payload)
            print(kin_user_response.status_code)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
