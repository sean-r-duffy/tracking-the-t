#!/opt/anaconda3/envs/tracking-the-t/bin/python

import requests
import gtfs_realtime_pb2
import json
import os
from datetime import datetime
import logging
from datetime import datetime

# MUST CHANGE THIS TO YOUR OWN LOCAL DIRECTORY
ABS_PATH = '/Users/seanduffy/PycharmProjects/tracking-the-t/'

logging.basicConfig(filename=ABS_PATH + 'data/api_fetches/logfile.log', level=logging.INFO)
logging.info(f"Script ran at {datetime.now()}")

def fetch_gtfs_realtime_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        response.raise_for_status()


def parse_gtfs_realtime_data(data):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)
    return feed


def save_predictions_to_directory(feed, directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(directory, f"predictions_{timestamp}.json")

    predictions = []
    for entity in feed.entity:
        if entity.trip_update:
            trip_update = {
                "trip_id": entity.trip_update.trip.trip_id,
                "start_time": entity.trip_update.trip.start_time,
                "start_date": entity.trip_update.trip.start_date,
                "stop_time_updates": []
            }
            for stop_time_update in entity.trip_update.stop_time_update:
                update = {
                    "stop_id": stop_time_update.stop_id,
                    "arrival": stop_time_update.arrival.time if stop_time_update.HasField("arrival") else None,
                    "departure": stop_time_update.departure.time if stop_time_update.HasField("departure") else None
                }
                trip_update["stop_time_updates"].append(update)
            predictions.append(trip_update)

    with open(filename, 'w') as f:
        json.dump(predictions, f, indent=2)

    print(f"Saved predictions to {filename}")
    logging.info(f"Saved predictions to {filename}")


def save_vehicle_locations_to_directory(feed, directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(directory, f"vehicle_locations_{timestamp}.json")

    vehicle_locations = []
    for entity in feed.entity:
        if entity.vehicle:
            vehicle = {
                "vehicle_id": entity.vehicle.vehicle.id,
                "trip_id": entity.vehicle.trip.trip_id,
                "latitude": entity.vehicle.position.latitude,
                "longitude": entity.vehicle.position.longitude,
                "bearing": entity.vehicle.position.bearing,
                "timestamp": entity.vehicle.timestamp
            }
            vehicle_locations.append(vehicle)

    with open(filename, 'w') as f:
        json.dump(vehicle_locations, f, indent=2)

    print(f"Saved vehicle locations to {filename}")
    logging.info(f"Saved vehicle locations to {filename}")


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
    logging.info(f"Saved forecast to {filename}")


def main():
    trip_updates_url = 'https://cdn.mbta.com/realtime/TripUpdates.pb'
    vehicle_positions_url = 'https://cdn.mbta.com/realtime/VehiclePositions.pb'
    weather_forecast_url = f"https://api.tomorrow.io/v4/weather/forecast?location=42.349706,-71.069855"
    trip_updates_directory = ABS_PATH + "data/api_fetches"
    vehicle_locations_directory = ABS_PATH + "data/api_fetches"
    forecasts_directory = ABS_PATH + "data/api_fetches"

    # Add your own tomorrow.io API Key
    with open(ABS_PATH + 'weather_api_key.txt') as file:
        weather_api_key = file.read()

    try:
        # Fetch and save trip updates
        trip_updates_data = fetch_gtfs_realtime_data(trip_updates_url)
        trip_updates_feed = parse_gtfs_realtime_data(trip_updates_data)
        save_predictions_to_directory(trip_updates_feed, trip_updates_directory)

        # Fetch and save vehicle locations
        vehicle_positions_data = fetch_gtfs_realtime_data(vehicle_positions_url)
        vehicle_positions_feed = parse_gtfs_realtime_data(vehicle_positions_data)
        save_vehicle_locations_to_directory(vehicle_positions_feed, vehicle_locations_directory)

        # Fetch weather forecast once per hour
        save_boston_forecast_to_directory(weather_forecast_url, forecasts_directory, weather_api_key)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()

