import requests
import gtfs_realtime_pb2
import gzip
import json
import os
from datetime import datetime


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


def main():
    url = 'https://cdn.mbta.com/realtime/TripUpdates.pb'
    output_directory = "predictions_data"

    try:
        data = fetch_gtfs_realtime_data(url)
        # If the data is gzipped, uncomment the next line
        # data = gzip.decompress(data)
        feed = parse_gtfs_realtime_data(data)
        save_predictions_to_directory(feed, output_directory)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
