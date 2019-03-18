from collections import namedtuple
from datetime import datetime, timedelta
import re

import psycopg2
import pytz

from config import ConnectionParams


TZ = pytz.timezone("US/Eastern")
_TripStop = namedtuple("_TripStop", ["stop_sequence",
                                     "stop_id",
                                     "arrival_time_raw",
                                     "departure_time_raw"])



def trip_stop_maker(dt):
    class TripStop(_TripStop):
        @property
        def arrival_time(self):
            return calculate_time(self.arrival_time_raw, dt)

        @property
        def departure_time(self):
            return calculate_time(self.departure_time_raw, dt)

    return TripStop


def calculate_time(timestr, dt=None):
    dt = dt or TZ.fromutc(datetime.utcnow())
    m = re.match(r"(\d\d):(\d\d):(\d\d)", timestr)
    arr_hours = int(m.group(1))
    return dt.replace(hour=arr_hours % 24,
                      minute=int(m.group(2)),
                      second=int(m.group(3)),
                      microsecond=0) + timedelta(days=int(arr_hours/24))


def stop_time(conn, trip_id, stop_seq, dt=None):
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT arrival_time, departure_time
        FROM stop_times
        WHERE trip_id = %(trip_id)s AND stop_sequence = %(stop_seq)s
        """, vars={
            "trip_id": trip_id,
            "stop_seq": stop_seq
        })

        arrival, departure = cursor.fetchone()
        return calculate_time(arrival, dt), calculate_time(departure, dt)


def stop_times(conn, trip_id, dt=None):
    TripStop = trip_stop_maker(dt or TZ.fromutc(datetime.utcnow()))
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT stop_sequence, stop_id, arrival_time, departure_time
        FROM stop_times
        WHERE trip_id = %(trip_id)s
        """, vars={"trip_id": trip_id})
        return {row[0]: TripStop(*row) for row in cursor}
