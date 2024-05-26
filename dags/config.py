import os 
from dotenv import load_dotenv 

load_dotenv() 
OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
