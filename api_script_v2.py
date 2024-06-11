#!/opt/anaconda3/envs/tracking-the-t/bin/python

import requests
import json
from datetime import datetime
import os
import logging

'''
This script is to be used for a cron job in order to collect api data at a set interval
Example cron job to collect every minute:
* * * * * /Users/seanduffy/PycharmProjects/tracking-the-t/api_script_v2.py
'''

# Constants
BASE_URL = "https://api-v3.mbta.com"
ROUTES = ['Red', 'Mattapan', 'Orange', 'Green-B', 'Green-C', 'Green-D', 'Green-E', 'Blue']
# MUST CHANGE THIS TO YOUR OWN LOCAL DIRECTORY
ABS_PATH = '/Users/seanduffy/PycharmProjects/tracking-the-t/'
with open(ABS_PATH + 'weather_api_key.txt') as file:
    WEATHER_API_KEY = file.read()
with open(ABS_PATH + 'mbta_api_key.txt') as file:
    MBTA_API_KEY = file.read()

logging.basicConfig(filename=ABS_PATH + 'data/mbta_v3/logfile.log', level=logging.INFO)
logging.info(f"Script ran at {datetime.now()}")


def fetch_data(endpoint, routes):

    url = f"{BASE_URL}/{endpoint}"
    params = {
        "filter[route]": ','.join(routes),
        "api_key": MBTA_API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def get_vehicle_data(routes):
    return fetch_data("vehicles", routes)


def get_prediction_data(routes):
    return fetch_data("predictions", routes)


def get_schedules_data(routes):
    return fetch_data("schedules", routes)


def collect_data():
    data = {
        "vehicles": get_vehicle_data(ROUTES),
        "predictions": get_prediction_data(ROUTES),
        "schedules": get_schedules_data(ROUTES)
    }
    return data


def save_data_to_file(data):
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    timestamp_str = now.strftime("%Y-%m-%d_%H-%M")

    # Create a subdirectory with the date
    os.makedirs(f'{ABS_PATH}data/mbta_v3/{date_str}', exist_ok=True)

    # File name with the full timestamp
    file_name = f'{ABS_PATH}data/mbta_v3/{date_str}/{timestamp_str}.json'

    with open(file_name, 'w') as file:
        json.dump(data, file, indent=4)

    print(f"Data saved to {file_name}")


def save_boston_forecast_to_directory(url, directory, api_key):
    if not os.path.exists(directory):
        os.makedirs(directory)

    timestamp = datetime.now().strftime("%Y%m%d_%H")
    filename = os.path.join(directory, f"boston_weather_{timestamp}.json")

    if os.path.exists(filename):
        return

    url = f"{url}&apikey={api_key}"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers).json()
    response_values = response["timelines"]["hourly"][0]["values"]
    needed_metrics = ['precipitationProbability', 'iceAccumulation', 'rainAccumulation', 'sleetAccumulation',
                      'snowAccumulation', 'snowDepth', 'temperature']
    forecast = {key: response_values[key] for key in needed_metrics if key in response_values}
    forecast["timestamp"] = timestamp

    with open(filename, 'w') as f:
        json.dump(forecast, f, indent=2)

    print(f"Saved forecast to {filename}")


if __name__ == "__main__":
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    weather_forecast_url = f"https://api.tomorrow.io/v4/weather/forecast?location=42.349706,-71.069855"
    forecasts_directory = f"{ABS_PATH}data/mbta_v3/weather"

    try:
        data = collect_data()
        save_data_to_file(data)
        save_boston_forecast_to_directory(weather_forecast_url, forecasts_directory, WEATHER_API_KEY)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
