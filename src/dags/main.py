import json
import sys
import os
import requests
import logging
from config import OPEN_WEATHER_API_KEY # colocar esse arquivo fora da pasta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Preciso usar para nao dar erro no import do config.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))           

def transform_weather_info(weather_list):
    pass

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
    
    def read_states_info() -> dict:
        logging.info("Reading states file.")
        logging.info(os.listdir("."))
        with open("src/states_info.json", "r") as file:
            states_info_dict = json.load(file)
        return states_info_dict

    @task
    def get_weather_info() -> list:
        states_info = read_states_info()
        weather_info_list = []
        for key, value in states_info.items():
            latitude = value["latitude"]
            longitute = value["longitude"]
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitute}&appid={OPEN_WEATHER_API_KEY}"
            response = requests.get(url)
            weather_info_list.append(response.json())
        logging.info("Extract task ran successfully!")
        return weather_info_list
    
    get_weather_info()
    
