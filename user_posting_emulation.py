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
        with open('pinterest_db_creds.yaml', 'r') as file:
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


class SendData():
    def __init__(self):
        self.max_iterations = 10 
        self.current_iteration = 0 
        
    def run_post_data_loop(self):
        while self.current_iteration < self.max_iterations:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()

            with engine.connect() as connection:
                pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_selected_row = connection.execute(pin_string)
                
                for row in pin_selected_row:
                    pin_result = dict(row._mapping)

                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_string)
                
                for row in geo_selected_row:
                    geo_result = dict(row._mapping)

                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_string)
                
                for row in user_selected_row:
                    user_result = dict(row._mapping)

                self.send_data_to_kafka_topic(pin_result, '0afff69adbe3.pin')
                self.send_data_to_kafka_topic(geo_result, '0afff69adbe3.geo')
                self.send_data_to_kafka_topic(user_result, '0afff69adbe3.user')
                
                self.current_iteration += 1 

            if self.current_iteration >= self.max_iterations:
                break

    def send_data_to_kafka_topic(self, data, topic_name):
        api_invoke_url = 'https://sqixei7ili.execute-api.us-east-1.amazonaws.com/newstage/topics'
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        payload = {
            "records": [{"value": data}]
        }
        response = requests.post(api_invoke_url + f'/{topic_name}', headers=headers, data=json.dumps(payload, default=str))
        if response.status_code == 200:
            print(f"Data sent to Kafka topic '{topic_name}' successfully.")
        else:
            print(f"Failed to send data to Kafka topic '{topic_name}'. Status code: {response.status_code}")


send_data_instance = SendData()


send_data_instance.run_post_data_loop()

if __name__ == "__main__":
    send_data_instance = SendData()
    send_data_instance.run_infinite_post_data_loop()
    print('Working')
    
    


