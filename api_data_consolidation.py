import pandas as pd
import json
import os
import re


def clean_predictions(df):
    df = df.explode('stop_time_updates')
    df = df.join(pd.json_normalize(df['stop_time_updates']))
    df = df.drop(columns=['stop_time_updates'])
    return df


def combine(predictions_df, locations_df, weather_df, timestamp='20240605_153000'):
    df = pd.merge(predictions_df, locations_df, on='trip_id', how='inner')
    df['collection_time'] = timestamp
    weather_df = weather_df.drop('timestamp', axis=1)
    weather_df['collection_time'] = timestamp
    df = pd.merge(df, weather_df, on='collection_time')
    return df


def rename_files_in_directory(directory):
    # Pattern to match the files with a full timestamp including seconds
    full_timestamp_pattern = re.compile(r"^(.*?_\d{8}_\d{4})(\d{2})(\.json)$")
    # Pattern to match the files with only hours in the timestamp
    hour_only_pattern = re.compile(r"^(.*?_\d{8}_\d{2})(\.json)$")

    for filename in os.listdir(directory):
        full_match = full_timestamp_pattern.match(filename)
        hour_match = hour_only_pattern.match(filename)

        if full_match:
            # Renaming files with a full timestamp to set seconds to '00'
            new_filename = f"{full_match.group(1)}00{full_match.group(3)}"
            old_filepath = os.path.join(directory, filename)
            new_filepath = os.path.join(directory, new_filename)
            os.rename(old_filepath, new_filepath)
            print(f"Renamed: {old_filepath} -> {new_filepath}")

        elif hour_match:
            # Renaming files with hours only to add '0000' for minutes and seconds
            new_filename = f"{hour_match.group(1)}0000{hour_match.group(2)}"
            old_filepath = os.path.join(directory, filename)
            new_filepath = os.path.join(directory, new_filename)
            os.rename(old_filepath, new_filepath)
            print(f"Renamed: {old_filepath} -> {new_filepath}")


def aggregate(date, save_to_parquet=False):
    df_list = []
    counter = 0
    for hour in range(15, 24):
        for minute in range(0, 60):
            time = f'{hour:02}{minute:02}00'
            timestamp = f'{date}_{time}'
            weather_timestamp = f'{date}_{hour:02}0000'

            predictions_path = f'{DATA_PATH}{date}/{PRED_PREFIX}{timestamp}.json'
            locations_path = f'{DATA_PATH}{date}/{LOC_PREFIX}{timestamp}.json'
            weather_path = f'{DATA_PATH}{date}/{WEATHER_PREFIX}{weather_timestamp}.json'

            try:
                predictions = pd.read_json(predictions_path)
                locations = pd.read_json(locations_path)
                with open(weather_path, 'r') as file:
                    weather = json.load(file)
                weather = [weather]
                weather = pd.DataFrame(weather)
                predictions = clean_predictions(predictions)

                min_df = combine(predictions, locations, weather, timestamp)
                df_list.append(min_df)
            except:
                print(f'Error with {timestamp}')

    df = pd.concat(df_list)
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df.to_parquet(f'{DATA_PATH}{date}.parquet')
    return df
