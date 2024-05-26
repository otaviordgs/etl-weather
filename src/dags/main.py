import json
import sys
import os
import boto3.resources
import requests
import logging
import boto3
import pandas as pd
from config import OPEN_WEATHER_API_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from airflow.models.dag import DAG
from airflow.decorators import task
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Preciso usar para nao dar erro no import do config.py
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))        

def read_states_info() -> dict:
        logging.info("Reading states file.")
        logging.info(os.listdir("."))
        with open("src/states_info.json", "r") as file:
            states_info_dict = json.load(file)
        return states_info_dict

def convert_kelvin_to_celsius(temp_kelvin: float) -> float:
        temp_celsius = temp_kelvin - 273.15
        return round(temp_celsius, 2)

def create_useful_info(weather_info: dict) -> dict:
    return {
        "city": weather_info["city"],
        "temperature": convert_kelvin_to_celsius(weather_info["main"]["temp"]),
        "temperature_min": convert_kelvin_to_celsius(weather_info["main"]["temp_min"]),
        "temperature_max": convert_kelvin_to_celsius(weather_info["main"]["temp_max"]),
        "feels_like": convert_kelvin_to_celsius(weather_info["main"]["feels_like"]),
        "description": weather_info["weather"][0]["description"],
        "date": datetime.today().strftime('%Y-%m-%d')
    }

with DAG(
    "etl_weather",
    default_args={
        "depends_on_past": False,
        "email": ["otaviodbz07@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Extract data about weather",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    @task()
    def get_weather_info() -> list:
        states_info = read_states_info()
        weather_info_list = []
        for key, value in states_info.items():
            latitude = value["latitude"]
            longitute = value["longitude"]
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitute}&appid={OPEN_WEATHER_API_KEY}"
            response = requests.get(url)
            response = response.json()
            response['city'] = key
            weather_info_list.append(response)
        logging.info("Extract task ran successfully!")
        return weather_info_list
    
    @task
    def transform_weather_info(ti = None):
        weathers_info_list = ti.xcom_pull(key='return_value', task_ids='get_weather_info')
        transformed_weather_list = []
        for weather_info in weathers_info_list:
            transformed_weather_list.append(create_useful_info(weather_info))

        ti.xcom_push(key='transformed_weather_list', value=transformed_weather_list)

    @task
    def send_data_to_s3(ti = None):
        s3 = boto3.resource(
            service_name='s3',
            region_name='sa-east-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        bucket = s3.Bucket('otavio-projects')

        transformed_weather_list = ti.xcom_pull(key='transformed_weather_list', task_ids='transform_weather_info')
        df = pd.DataFrame(transformed_weather_list)
        df.to_csv("weather_info.csv")
        try:
            bucket.upload_file("weather_info.csv", "weather_info.csv")
        except ClientError as e:
            logging.error(e)
        finally:
            os.remove("weather_info.csv")
            logging.info(os.getcwd())
        logging.info("File uploaded successfully!")     

    get_weather_info() >> transform_weather_info() >> send_data_to_s3()
    
# TO DO: ver como passar informações entre as tasks
