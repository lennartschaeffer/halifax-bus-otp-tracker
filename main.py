import requests
from google.transit import gtfs_realtime_pb2
import time

# Vehicle Positions API
url = 'https://gtfs.halifax.ca/realtime/Vehicle/VehiclePositions.pb'
response = requests.get(url)
feed = gtfs_realtime_pb2.FeedMessage() #type: ignore
feed.ParseFromString(response.content)

vehiclePositions = []

for entity in feed.entity:
    vehiclePositions.append(entity)

with open('vehiclePositions.txt', 'w') as f:
    for update in vehiclePositions:
        f.write(str(update))
        f.write('\n')
print(f"Saved {len(vehiclePositions)} trip updates to vehiclePositions.txt")

# Trip Updates API
url = 'https://gtfs.halifax.ca/realtime/TripUpdate/TripUpdates.pb'
response = requests.get(url)
feed = gtfs_realtime_pb2.FeedMessage() #type: ignore
feed.ParseFromString(response.content)

tripUpdates = []

for entity in feed.entity:
    tripUpdates.append(entity)

with open('tripUpdates.txt', 'w') as f:
    for update in tripUpdates:
        f.write(str(update))
        f.write('\n')
print(f"Saved {len(tripUpdates)} trip updates to tripUpdates.txt")

# Alerts API
url = 'https://gtfs.halifax.ca/realtime/Alert/Alerts.pb'
response = requests.get(url)
feed = gtfs_realtime_pb2.FeedMessage() #type: ignore
feed.ParseFromString(response.content)

alerts = []

for entity in feed.entity:
    alerts.append(entity)

with open('alerts.txt', 'w') as f:
    for update in alerts:
        f.write(str(update))
        f.write('\n')
print(f"Saved {len(alerts)} trip updates to alerts.txt")

