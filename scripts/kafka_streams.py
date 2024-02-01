import json
import uuid
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from time import sleep
from datetime import datetime, timedelta
import requests

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")

class WeatherDataGenerator(object):
    @staticmethod
    def get_weather_data(city_name):
        api_key = "cfd0801520690aec20116cd5a4cb9525"
        api_url = f'http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric'
        
        response = requests.get(api_url)
        
        if response.status_code == 200:
            weather_data = response.json()

            return {
                "City": weather_data.get('name'),
                "Weather_Condition": weather_data['weather'][0]['main'],
                "Temperature_Celsius": weather_data['main']['temp'],
                "Humidity_Percentage": weather_data['main']['humidity'],
                "Wind_Speed": weather_data['wind']['speed'],
                "Timestamp": datetime.utcfromtimestamp(weather_data.get('dt')).strftime('%Y-%m-%d %H:%M:%S UTC'),
            }
        else:
            return None

# Major cities in Taiwan
taiwan_cities = ["Taipei", "New Taipei", "Taichung", "Tainan", "Kaohsiung", "Taoyuan", "Keelung"]

while True:
    columns = [
        "ID",
        "City",
        "Weather_Condition",
        "Temperature_Celsius",
        "Humidity_Percentage",
        "Wind_Speed",
        "Timestamp",
    ]

    for city_name in taiwan_cities:
        weather_data = WeatherDataGenerator.get_weather_data(city_name)

        if weather_data is not None:
            json_data = dict(zip(columns, [uuid.uuid4().__str__()] + list(weather_data.values())))
            _payload = json.dumps(json_data).encode("utf-8")

            print(_payload, flush=True)
            print("=-" * 5, flush=True)

            response = producer.send(topic=kafka_topic, value=_payload)
            print(response.get())
            print("=-" * 20, flush=True)

        sleep(10) 
