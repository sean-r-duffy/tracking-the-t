import pandas as pd
import json
import os
import matplotlib.pyplot as plt

DIRECTORY = 'data/mbta_v3'


def open_as_df(file_path, key):
    with open(file_path, 'r') as file:
        data = json.load(file)
        df = pd.json_normalize(data[key]['data'])
    return df


def clean(df, key):
    if key == 'vehicles':
        columns = ['id', 'attributes.bearing', 'attributes.current_status', 'attributes.current_stop_sequence',
                   'attributes.direction_id', 'attributes.latitude', 'attributes.longitude', 'attributes.revenue',
                   'attributes.updated_at', 'relationships.route.data.id', 'relationships.stop.data.id',
                   'relationships.trip.data.id']
        new_column_names = {
            'id': 'Vehicle ID',
            'attributes.bearing': 'Bearing',
            'attributes.current_status': 'Current Status',
            'attributes.current_stop_sequence': 'Current Stop Sequence',
            'attributes.direction_id': 'Direction ID',
            'attributes.latitude': 'Latitude',
            'attributes.longitude': 'Longitude',
            'attributes.revenue': 'Revenue',
            'attributes.updated_at': 'Last Updated',
            'relationships.route.data.id': 'Route ID',
            'relationships.stop.data.id': 'Stop ID',
            'relationships.trip.data.id': 'Trip ID'
        }
    elif key == 'predictions':
        columns = ['id', 'attributes.arrival_time', 'attributes.arrival_uncertainty', 'attributes.departure_time',
                   'attributes.departure_uncertainty', 'attributes.direction_id', 'attributes.revenue',
                   'attributes.schedule_relationship', 'attributes.status', 'attributes.stop_sequence',
                   'attributes.update_type', 'relationships.route.data.id', 'relationships.stop.data.id',
                   'relationships.trip.data.id', 'relationships.vehicle.data.id']
        new_column_names = {
            'id': 'Prediction ID',
            'attributes.arrival_time': 'Arrival Time',
            'attributes.arrival_uncertainty': 'Arrival Uncertainty',
            'attributes.departure_time': 'Departure Time',
            'attributes.departure_uncertainty': 'Departure Uncertainty',
            'attributes.direction_id': 'Direction ID',
            'attributes.revenue': 'Revenue',
            'attributes.schedule_relationship': 'Schedule Relationship',
            'attributes.status': 'Status',
            'attributes.stop_sequence': 'Stop Sequence',
            'attributes.update_type': 'Update Type',
            'relationships.route.data.id': 'Route ID',
            'relationships.stop.data.id': 'Stop ID',
            'relationships.trip.data.id': 'Trip ID',
            'relationships.vehicle.data.id': 'Vehicle ID'
        }
    elif key == 'schedules':
        columns = ['id', 'attributes.arrival_time', 'attributes.departure_time', 'attributes.direction_id',
                   'attributes.stop_sequence', 'attributes.timepoint', 'relationships.route.data.id',
                   'relationships.stop.data.id', 'relationships.trip.data.id']
        new_column_names = {
            'id': 'Schedule ID',
            'attributes.arrival_time': 'Arrival Time',
            'attributes.departure_time': 'Departure Time',
            'attributes.direction_id': 'Direction ID',
            'attributes.stop_sequence': 'Stop Sequence',
            'attributes.timepoint': 'Timepoint',
            'relationships.route.data.id': 'Route ID',
            'relationships.stop.data.id': 'Stop ID',
            'relationships.trip.data.id': 'Trip ID'
        }
    else:
        raise Exception('Key must be one of ["vehicles", "predictions", "schedules"]')

    clean_df = df[columns]
    clean_df = clean_df.rename(columns=new_column_names)
    return clean_df


def load_to_df(file_path, key):
    df = open_as_df(file_path, key)
    df = clean(df, key)

    return df


def aggregate_by_key(date, key, start_time=None, end_time=None):
    all_dfs = []
    directory_path = f'{DIRECTORY}/{date}'
    if start_time is None:
        start_time = '00-00'
    if end_time is None:
        end_time = '24-00'
    start_time = start_time.replace(':', '-').zfill(5)
    end_time = end_time.replace(':', '-').zfill(5)

    for filename in os.listdir(directory_path):
        time = filename[11:16]
        if filename.endswith(".json") and (start_time <= time <= end_time):
            file_path = os.path.join(directory_path, filename)
            df = load_to_df(file_path, key)
            df['Collection Time'] = f'{date}_{time}'
            all_dfs.append(df)

    df = pd.concat(all_dfs, ignore_index=True)
    return df


def join_dfs(vehicles_df, predictions_df):
    df = vehicles_df.merge(predictions_df, left_on=['Current Stop Sequence', 'Trip ID'],
                           right_on=['Stop Sequence', 'Trip ID'], how='inner', suffixes=(' Vehicle', ' Prediction'))
    df = df[df['Current Status'] == 'STOPPED_AT']
    df = df.dropna(subset='Arrival Time')
    df['Arrival Time'] = pd.to_datetime(df['Arrival Time'])
    df['Departure Time'] = pd.to_datetime(df['Departure Time'])
    df['Last Updated'] = pd.to_datetime(df['Last Updated'])
    df['Collection Time Prediction'] = pd.to_datetime(df['Collection Time Prediction'], format='%Y-%m-%d_%H-%M')
    df['Collection Time Prediction'] = df['Collection Time Prediction'].dt.tz_localize('America/New_York')
    df['Prediction Error'] = df['Arrival Time'] - df['Last Updated']
    df['Absolute Prediction Error'] = abs(df['Prediction Error'])
    df['Prediction Frame'] = df['Arrival Time'] - df['Collection Time Prediction']
    bins = [pd.Timedelta(minutes=0), pd.Timedelta(minutes=3), pd.Timedelta(minutes=6), pd.Timedelta(minutes=10),
            pd.Timedelta(minutes=15)]
    labels = ['0-3 min', '3-6 min', '6-10 min', '10-15 min']
    df['Bucket'] = pd.cut(df['Prediction Frame'], bins=bins, labels=labels, include_lowest=True)
    lower_bound_dict = {
        '0-3 min': pd.Timedelta('-00:00:30'),
        '3-6 min': pd.Timedelta('-00:01:00'),
        '6-10 min': pd.Timedelta('-00:01:00'),
        '10-15 min': pd.Timedelta('-00:01:30')
    }
    upper_bound_dict = {
        '0-3 min': pd.Timedelta('00:01:30'),
        '3-6 min': pd.Timedelta('00:02:30'),
        '6-10 min': pd.Timedelta('00:03:30'),
        '10-15 min': pd.Timedelta('00:04:30')
    }
    df['Lower Tolerance'] = df['Bucket'].map(lower_bound_dict)
    df['Upper Tolerance'] = df['Bucket'].map(upper_bound_dict)
    df['Accurate'] = ((df['Prediction Error'] >= df['Lower Tolerance']) &
                      (df['Prediction Error'] <= df['Upper Tolerance']))

    return df


def accuracy_table(df):
    total_counts = pd.pivot_table(df,
                                  values='Accurate',
                                  index='Route ID Vehicle',
                                  columns='Bucket',
                                  aggfunc='count',
                                  fill_value=0)
    accurate_counts = pd.pivot_table(df[df['Accurate']],
                                     values='Accurate',
                                     index='Route ID Vehicle',
                                     columns='Bucket',
                                     aggfunc='count',
                                     fill_value=0)
    percentage_accurate = round(accurate_counts / total_counts * 100)
    percentage_accurate['Average Accuracy'] = percentage_accurate.mean(axis=1)
    overall_mean = percentage_accurate.mean()
    percentage_accurate.loc['Overall'] = round(overall_mean)

    return percentage_accurate


def plot_prediction_error(df):
    plot_df = df.copy()
    plot_df['Prediction Frame'] = plot_df['Prediction Frame'].dt.total_seconds() / 60
    plot_df['Prediction Error'] = plot_df['Prediction Error'].dt.total_seconds() / 60
    plot_df = plot_df[(plot_df['Prediction Frame'] > 0) & (plot_df['Prediction Frame'] <= 15)]

    route_colors = {
        'Blue': '#003DA5',
        'Red': '#DA291C',
        'Mattapan': '#DA291C',
        'Green-B': '#00843D',
        'Green-C': '#00843D',
        'Green-D': '#00843D',
        'Green-E': '#00843D',
        'Orange': '#ED8B00'
    }

    # Ensure the 'Route ID' is treated as a categorical variable
    plot_df['Route ID'] = plot_df['Route ID Vehicle'].astype('category')

    # Map the Route ID to the corresponding colors
    colors = plot_df['Route ID'].map(route_colors)

    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(plot_df['Prediction Frame'], plot_df['Prediction Error'], c=colors)
    ax.set_ylabel('Arrival Prediction Error')
    ax.set_xlabel('Prediction Frame')
    ax.grid(True)
    ax.invert_xaxis()

    # Create a legend
    handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10) for color in
               route_colors.values()]
    labels = route_colors.keys()
    ax.legend(handles, labels, title="Route")

    return fig


def load_full_df(date, start_time=None, end_time=None):
    vehicles_df = aggregate_by_key(date, 'vehicles', start_time=start_time, end_time=end_time)
    predictions_df = aggregate_by_key(date, 'predictions', start_time=start_time, end_time=end_time)
    df = join_dfs(vehicles_df, predictions_df)

    return df
