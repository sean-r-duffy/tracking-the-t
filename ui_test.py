import tkinter as tk
from tkinter import ttk
import pandas as pd
import requests
from datetime import datetime

# Tkinter code pulled from online

def set_up_api():
    with open('api_key.txt') as file:
        api_key = file.read()
    stops = requests.get('https://api-v3.mbta.com/stops', params={'filter[route]': 'Green-D'}).json()
    options = []
    for stop in stops['data']:
        options.append(stop['id'])
    return options, api_key

def check_stop(stop_id, api_key):
    response = requests.get(f'https://api-v3.mbta.com/predictions?filter[stop]={stop_id}&sort=arrival_time&api_key={api_key}').json()
    out_string = ""
    for instance in response['data']:
        vehicle_id = instance['relationships']['vehicle']['data']['id']
        arrival_time = instance['attributes']['arrival_time']
        arrival_time = datetime.fromisoformat(arrival_time).strftime("%I:%M %p")
        departure_time = instance['attributes']['departure_time']
        departure_time = datetime.fromisoformat(departure_time).strftime("%I:%M %p")
        direction = instance['attributes']['direction_id']
        route_id = instance['relationships']['route']['data']['id']
        trip_id = instance['relationships']['trip']['data']['id']
        out_string += f"vehicle: {vehicle_id} || arrival: {arrival_time} || departure: {departure_time} || direction: {direction} || route: {route_id} || trip: {trip_id}\n"
    return out_string

def on_select(event):
    selected_option = str(combo.get())
    predictions = check_stop(selected_option, api_key)
    output_text.set(predictions)

root = tk.Tk()
root.title("Check Stop")

frame = ttk.Frame(root, padding="20 20 20 20")
frame.grid(column=0, row=0, sticky=(tk.W, tk.E, tk.N, tk.S))

options, api_key  = set_up_api()

output_text = tk.StringVar()

combo = ttk.Combobox(frame, values=options)
combo.grid(column=0, row=0, sticky=(tk.W, tk.E))
combo.bind("<<ComboboxSelected>>", on_select)

output_label = ttk.Label(frame, textvariable=output_text, width=75)
output_label.grid(column=0, row=1)

output_text.set("Select an option from the dropdown menu")

for child in frame.winfo_children():
    child.grid_configure(padx=5, pady=5)

frame.columnconfigure(0, weight=1)
frame.rowconfigure(0, weight=1)
frame.rowconfigure(1, weight=1)

root.mainloop()
