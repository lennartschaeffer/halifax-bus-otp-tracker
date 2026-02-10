import requests
from google.transit import gtfs_realtime_pb2

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
