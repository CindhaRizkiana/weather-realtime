import json
import uuid
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from time import sleep
from datetime import datetime, timedelta
from faker import Faker

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
fake = Faker()

class WeatherDataGenerator(object):
    @staticmethod
    def get_fake_weather_data(city_name):
        weather_conditions = ["Clear", "Cloudy", "Rainy", "Snowy", "Windy", "Foggy"]

        return {
            "City": city_name,
            "Weather_Condition": fake.random_element(weather_conditions),
            "Temperature_Celsius": fake.pyfloat(left_digits=2, right_digits=1, positive=True),
            "Humidity_Percentage": fake.pyfloat(left_digits=2, right_digits=1, positive=True),
            "Wind_Speed": fake.pyfloat(left_digits=2, right_digits=1, positive=True),
            "Timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
        }

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
        try:
            weather_data = WeatherDataGenerator.get_fake_weather_data(city_name)

            json_data = dict(zip(columns, [uuid.uuid4().__str__()] + list(weather_data.values())))
            payload = json.dumps(json_data).encode("utf-8")

            print(payload, flush=True)
            print("=-" * 5, flush=True)

            producer.send(topic=kafka_topic, value=payload)
            print("Message sent successfully.")
            print("=-" * 20, flush=True)
        except Exception as e:
            print(f"Error sending message: {e}")

    sleep(10)
