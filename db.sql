CREATE DATABASE mbta;

CREATE TABLE IF NOT EXISTS vehicle_updates (
trip_id             char(50),
trip_start          date,
stop_id             char(50),
stop_sequence       integer,
vehicle_id          char(50),
status              char(50),
timestamp           timestamp,
lat                 double precision,
lon                 double precision
-- scheduled_arrival   timestamp,
-- At what time was the vehicle "incoming" to the stop?
-- incoming            timestamp null,
-- At what time was the vehicle stopped at the stop
-- stopped_at          timestamp null
);

-- Disregard duplicate timestamp entries:
CREATE UNIQUE INDEX IF NOT EXISTS trip_stop_timestamp_idx ON vehicle_updates (trip_id, trip_start, stop_id, timestamp);
CREATE INDEX IF NOT EXISTS trip_start ON vehicle_updates (trip_start);
CREATE INDEX IF NOT EXISTS trip_instance_idx ON vehicle_updates (trip_id, trip_start);
CREATE INDEX IF NOT EXISTS trip_id_idx ON vehicle_updates (trip_id);
CREATE INDEX IF NOT EXISTS stop_sequence_idx ON vehicle_updates (stop_sequence);
CREATE INDEX IF NOT EXISTS status_idx ON vehicle_updates (status);
-- CREATE INDEX IF NOT EXISTS incoming_idx ON vehicle_updates (incoming DESC);
-- CREATE INDEX IF NOT EXISTS stopped_at_idx ON vehicle_updates (stopped_at DESC);

CREATE TABLE IF NOT EXISTS stop_times (
  trip_id         varchar(50),
  stop_id         varchar(50),
  stop_sequence   integer,
  arrival_time    varchar(25),
  departure_time  varchar(25)
);
CREATE UNIQUE INDEX IF NOT EXISTS trip_stop_time ON stop_times (trip_id, stop_sequence);
CREATE INDEX IF NOT EXISTS trip_stop_times_idx ON stop_times (trip_id);


CREATE TABLE IF NOT EXISTS alerts (
id char(50),
header text,
effect char(20)
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_id_pk_idx ON alerts (id);

-- 
CREATE TABLE IF NOT EXISTS alert_targets (
alert_id char(50),
route_id char(50) null,
route_type int null,
stop_id char(50) null
);

-- CREATE INDEX IF NOT EXISTS 

CREATE TABLE IF NOT EXISTS alert_periods (
alert_id char(50,)
);
