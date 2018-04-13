from collections import defaultdict
import csv
from datetime import datetime
import time
import os

from urllib.error import URLError
from urllib.request import urlopen

from gtfs_realtime_pb2 import Alert, FeedMessage, VehiclePosition


VehiclePositionsUrl = "http://developer.mbta.com/lib/GTRTFS/Alerts/VehiclePositions.pb"
AlertsUrl = "http://developer.mbta.com/lib/GTRTFS/Alerts/Alerts.pb"

DEFAULT_BASE_DIR = os.path.join(os.path.dirname(__file__), "updates")
BASE_DIR = os.environ.get("MBTA_UPDATES_DIR", DEFAULT_BASE_DIR)


def get_vehicle_positions(url=VehiclePositionsUrl):
    try:
        with urlopen(url) as u:
            message = FeedMessage()
            message.ParseFromString(u.read())
            return filter(None, (entity.vehicle for entity in message.entity))
    except URLError:
        return []


def get_alerts(url=AlertsUrl):
    try:
        with urlopen(url) as u:
            message = FeedMessage()
            message.ParseFromString(u.read())
            return message.entity
    except URLError:
        return None


def get_translation(translated_string, lang="en"):
    for translation in translated_string.translation:
        if translation.language == lang:
            return translation.text

# Create a new alert
# For each affected trip, stop, or route, insert a new entry in alert_target
# For each affected period, create a new entry in alert_periods
def make_alert_dict(alert_entity):
    alert = alert_entity.alert
    return {"alert_id": alert_entity.id,
            "header": get_translation(alert.header_text),
            "description": get_translation(alert.description_text),
            "effect": Alert.Effect.Name(alert.effect)}


def format_start(trip_start):
    return trip_start[0:4] + "-" + trip_start[4:6] + "-" + trip_start[6:]


def make_update_dict(vehicle):
    trip = vehicle.trip
    pos = vehicle.position
    status = VehiclePosition.VehicleStopStatus.Name(vehicle.current_status)
    return {"trip_id": trip.trip_id,
            "trip_start": format_start(trip.start_date),
            "stop_id": vehicle.stop_id,
            "stop_sequence": vehicle.current_stop_sequence,
            "vehicle_id": vehicle.vehicle.id,
            "status": status,
            "timestamp": datetime.fromtimestamp(vehicle.timestamp),
            "lat": pos.latitude,
            "lon": pos.longitude}


def store_latest_updates():
    grouped_updates = defaultdict(list)
    for vehicle in get_vehicle_positions():
        update_dict = make_update_dict(vehicle)
        grouped_updates[update_dict["trip_start"]].append(update_dict)

    for trip_start, updates in grouped_updates.items():
        with open(os.path.join(BASE_DIR, f"{trip_start}.csv"), "a") as outfile:
            writer = csv.DictWriter(outfile,
                                    ["trip_id", "trip_start", "stop_id",
                                     "stop_sequence", "vehicle_id", "status",
                                     "timestamp", "lat", "lon"])
            if outfile.tell() == 0:
                writer.writeheader()
            writer.writerows(updates)


def run():
    try:
        while True:
            store_latest_updates()
            time.sleep(15)
    except KeyboardInterrupt:
        print("Exiting")


if __name__ == "__main__":
    run()
