import api_data_consolidation as adc
import requests
import pandas as pd
import json
from datetime import datetime
import pytz

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

with open('mbta_stop_mapping.json', 'r') as json_file:
    mbta_stop_mapping = json.load(json_file)

with open('mbta_api_key.txt') as file:
    MBTA_API_KEY = file.read()

direction_dict = {'Red': {0: 'Ashmont/Braintree', 1: 'Alewife'},
                  'Mattapan': {0: 'Mattapan', 1: 'Ashmont'},
                  'Orange': {0: 'Forest Hills', 1: 'Oak Grove'},
                  'Green-B': {0: 'Boston College', 1: 'Government Center'},
                  'Green-C': {0: 'Cleveland Circle', 1: 'Government Center'},
                  'Green-D': {0: 'Riverside', 1: 'Union Square'},
                  'Green-E': {0: 'Heath Street', 1: 'Medford/Tufts'},
                  'Blue': {0: 'Bowdoin', 1: 'Wonderland'}}
def get_predictions(stop_id, num_predictions):
    """
    Retrieve the specified number of predictions for a stop given the stop ID.
    Only retrieves predictions for light rail and subway, and filters out predictions
    with arrival times earlier than the current time.

    Parameters:
    stop_id (str): The stop ID.
    num_predictions (int): The number of predictions to retrieve.

    Returns:
    list: A list of dictionaries containing the predictions.
    """
    headers = {
        'api-key': 'YOUR_MBTA_API_KEY'  # Replace with your MBTA API key
    }

    params = {
        'filter[stop]': stop_id,
        'filter[route_type]': '0,1',  # 0 for Light Rail, 1 for Subway
        'sort': 'arrival_time'
    }

    url = "https://api-v3.mbta.com/predictions"

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    predictions = data.get('data', [])

    # Extract the specified number of predictions
    selected_predictions = []
    for prediction in predictions:
        arrival_time_str = prediction['attributes']['arrival_time']
        if arrival_time_str:
            arrival_time = datetime.fromisoformat(arrival_time_str.rstrip('Z'))
            now = datetime.now(pytz.utc)
            if arrival_time >= now:
                prediction_details = {
                    'route_id': prediction['relationships']['route']['data']['id'],
                    'stop_id': prediction['relationships']['stop']['data']['id'],
                    'arrival_time': arrival_time_str,
                    'direction_id': prediction['attributes']['direction_id']
                }
                selected_predictions.append(prediction_details)

        if len(selected_predictions) >= num_predictions:
            break

    return selected_predictions


def print_predictions(stop_id, num_predictions):
    predictions = get_predictions(stop_id, num_predictions)
    for prediction in predictions:
        stop_info = mbta_stop_mapping[prediction['stop_id']][1]
        arr_time = datetime.fromisoformat(prediction['arrival_time'])
        route_id = prediction['route_id']
        now = datetime.now(pytz.utc)
        difference = arr_time - now
        minutes_difference = round(difference.total_seconds() / 60)
        print(f'{minutes_difference} minutes away | {stop_info} | {route_id}')


def main():
    print('Welcome!')
    view_pred = input('View Prediction Accuracy? [Y/N]: ')
    view_pred = view_pred.lower().strip() == 'y'
    if view_pred:
        df = adc.load_full_df('2024-06-10', '12:00', '12:15')
        fig = adc.plot_prediction_error(df)
        print(adc.accuracy_table(df))
        fig.show()
        print('\n\n')

    stop_id = input('Stop ID: ')
    num_predictions = int(input('# Trains: '))

    print(f'Nearest {num_predictions} Trains: ')
    print_predictions(stop_id, num_predictions)

if __name__ == '__main__':
    main()
