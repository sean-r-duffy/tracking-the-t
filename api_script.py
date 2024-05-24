import requests
import json
from datetime import datetime
from pathlib import Path

with open('api_key.txt') as f:
    API_KEY = f.read()
BASE_URL = 'https://api-v3.mbta.com'
DIRECTORY = Path.cwd() / 'data/api_fetches'


def fetch_data(endpoint, params=None):
    headers = {'api-key': API_KEY}
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {endpoint}: {e}")
        return None


def save_json(data, directory_path, filename):
    if not directory_path.exists():
        directory_path.mkdir(parents=True)

    filepath = directory_path / filename
    with filepath.open('w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"Data saved to {filepath}")


def get_predictions(route_types):
    data = fetch_data('predictions', params={'filter[route_type]': str(route_types)})
    return data['data']


def get_vehicle_locations(route_types):
    data = fetch_data('vehicles', params={'filter[route_type]': str(route_types)})
    return data['data']


def main():
    route_types = [0, 1, 2, 3]  # Light Rail, Heavy Rail, Commuter Rail, Bus

    # Fetch predictions for each stop
    predictions = get_predictions(route_types)
    if predictions:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"predictions_{timestamp}.json"
        save_json(predictions, DIRECTORY, filename)

    vehicles = get_vehicle_locations(route_types)
    if vehicles:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"vehicle_locations_{timestamp}.json"
        save_json(vehicles, DIRECTORY, filename)


if __name__ == "__main__":
    main()
