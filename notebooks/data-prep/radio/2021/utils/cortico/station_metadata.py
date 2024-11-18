#!/usr/bin/env python3

"""
Holds all the metadata associated with radio stations from radio-locator.
This is loaded from a yaml file in s3 and stored in memory, with overrides
(manually added stations) in a file in the repo.  This metadata is used
at various points in recognition post-processing, indexing, and API serving.
"""

import calendar
import csv
import boto3
import logging
import pytz
from yaml import safe_load as yaml_load

from . import radiosearch_lib

STATIONS_YML_BUCKET = "cortico-adhoc-data"
STATIONS_YML_KEY = "talk-radio/all_stations.yml"
EXTRA_STATIONS_FILE = "config/extra_stations.yml"

# Schedule data.  Contains a map of (station callsign, time interval) -> show name
SHOW_SCHEDULES_CSV_FILES = [
    "config/station_schedules.csv",
    "config/station_schedules_additions.csv",
]

# Contains timezones for all possible stations.  Neded for accurate determination
# of which talk show is airing on the station at a particular time
STATION_TIMEZONE_FILE = "config/station_timezones.csv"

# If provided, override the schedules with corrections in this file.
# The corrections is an auto-generated overlay based on
# tools/radio_shows/infer_schedule_updates.py
SHOW_SCHEDULES_CORRECTIONS_FILE = None

# The days of the week in the schedules file above must match these
_DAY_NAMES = [x.lower() for x in list(calendar.day_name)]
_MINUTES_IN_DAY = 24 * 60
_MINUTES_IN_WEEK = _MINUTES_IN_DAY * 7

# From Clara, this maps the source of a scraped schedule to an estimate of its reliability
_SOURCE_CONFIDENCES = {
    "dar": 0.48,
    "iheart": 0.78,
    "npr": 0.89,
    "other": 0.7,
    "pub": 0.67,
    "auto_fill_1": 0.56,
    "auto_fill_2": 0.66,
    "auto_fill_3": 0.75,
    "auto_fill_4": 0.82,
    "auto_fill_5": 0.88,
    "auto_fill_6": 0.91,
    "auto_fill_7": 0.94,
    "auto_fill_8": 0.96,
    "auto_fill_9": 0.97,
    "auto_fill_10": 0.98,
    "auto_fill_11": 0.99,
    "auto_fill_12": 0.99,
}


class StationMetadata:
    def __init__(self, logger=None, base_schedules_only=False):
        self.station_metadata = {}
        self.logger = logger
        # Read base radio station data
        s3_resource = boto3.resource("s3", region_name=radiosearch_lib.REGION_NAME)
        obj = s3_resource.Object(STATIONS_YML_BUCKET, STATIONS_YML_KEY)
        content = obj.get()["Body"].read().decode("utf-8")
        stations = yaml_load(content)
        # Read manually-added extra stations
        extra_stations = yaml_load(open(EXTRA_STATIONS_FILE).read())
        for station in stations + extra_stations:
            if "latlong_degrees" in station:
                if station["latlong_degrees"] is not None:
                    lat, lon = latlon_string_to_tuple(station["latlong_degrees"])
                    station["antenna_lat"] = lat
                    station["antenna_lon"] = lon
                    del station["latlong_degrees"]
            self.station_metadata[station.get("callsign", "unknown")] = station
        # Read show schedules
        self.callsign_minute_to_show = (
            {}
        )  # (callsign, minute_of_week) -> (show_name, source, conf)
        self.show_to_stations = {}  # show_name -> callsign set
        self.callsign_to_timezone = {}  # show_name -> callsign set
        self._read_station_timezone_data(STATION_TIMEZONE_FILE)
        for fn in SHOW_SCHEDULES_CSV_FILES:
            self._read_schedule_data(fn)
        if not base_schedules_only and SHOW_SCHEDULES_CORRECTIONS_FILE:
            self._read_schedule_data(SHOW_SCHEDULES_CORRECTIONS_FILE)

    @staticmethod
    def _time_to_week_minute(day_num, hour, minute):
        return day_num * _MINUTES_IN_DAY + hour * 60 + minute

    @staticmethod
    def _time_str_to_week_minute(day_num, time_str):
        hour, minute, _ = [int(x) for x in time_str.split(":")]
        return StationMetadata._time_to_week_minute(day_num, hour, minute)

    def _read_station_timezone_data(self, filename):
        with open(filename) as csvin:
            reader = csv.DictReader(csvin)
            for row in reader:
                self.callsign_to_timezone[row["callsign"]] = row["timezone"]

    def _read_schedule_data(self, filename):
        with open(filename) as csvin:
            reader = csv.DictReader(csvin)
            for row in reader:
                callsign, show_name, source = (
                    row["callsign"],
                    row["show"],
                    row["source"],
                )
                station_timezone = row["station_timezone"]
                if station_timezone != "NA":
                    # Overwrite existing timezone with timezone from schedule data
                    self.callsign_to_timezone[callsign] = station_timezone

                current_station_set = self.show_to_stations.get(show_name, set())
                current_station_set.add(callsign)

                self.show_to_stations[show_name] = current_station_set
                if not show_name:
                    if self.logger:
                        self.logger.info("WARNING:  No show_name", str(row))
                    continue
                if "confidence" in row:
                    confidence = float(row["confidence"])
                else:
                    confidence = StationMetadata.schedule_source_confidence(source)
                # Just use UTC fields to find duration, but store schedules
                # according to local time
                start_day = row["start_day_utc"]
                end_day = row["end_day_utc"]
                start_minute = self._time_str_to_week_minute(
                    _DAY_NAMES.index(start_day), row["start_time_utc"]
                )
                end_minute = self._time_str_to_week_minute(
                    _DAY_NAMES.index(end_day), row["end_time_utc"]
                )
                # Some shows end at :59 in the schedule; bump to the following hour
                if row["end_time_utc"].endswith(":59:00"):
                    end_minute = (end_minute + 1) % _MINUTES_IN_WEEK

                # Temporarily use this duration, and rewrite everything in local time
                # TODO(dougb):  The schedule file should only have local tz
                duration_minutes = end_minute - start_minute
                start_minute = self._time_str_to_week_minute(
                    _DAY_NAMES.index(row["start_day_tz"]), row["start_time_tz"]
                )
                end_minute = (start_minute + duration_minutes) % _MINUTES_IN_WEEK

                if (end_minute - start_minute) % _MINUTES_IN_WEEK > _MINUTES_IN_DAY:
                    if self.logger:
                        self.logger.info(
                            "WARNING:  Show %s for %s is > 24 hours in schedule; "
                            + "skipping:",
                            show_name,
                            callsign,
                        )
                        self.logger.info("   " + ",".join(row.values()))
                    continue

                overlapping_shows = set()
                for x in range(
                    start_minute,
                    (end_minute < start_minute and _MINUTES_IN_WEEK or end_minute),
                ):
                    if (callsign, x) in self.callsign_minute_to_show:
                        overlapping_show = self.callsign_minute_to_show.get(
                            (callsign, x)
                        )
                        if (
                            self.logger
                            and overlapping_show != show_name
                            and overlapping_show not in overlapping_shows
                        ):
                            self.logger.warn(
                                "WARNING:  Overwriting row that contradicts prior row"
                            )
                            self.logger.warn("   Row: " + ",".join(row.values()))
                            self.logger.warn(
                                "   Overlaps with prior row for show:  "
                                + str(overlapping_show)
                            )
                            overlapping_shows.add(overlapping_show)
                    # Only select this if it's higher confidence than what we already have.
                    if not ((callsign, x) in self.callsign_minute_to_show) or (
                        confidence >= self.callsign_minute_to_show[(callsign, x)][2]
                    ):
                        self.callsign_minute_to_show[(callsign, x)] = (
                            show_name,
                            source,
                            confidence,
                        )
                # If the end time happens to be in the following week...
                if end_minute < start_minute:
                    for x in range(0, end_minute):
                        if not ((callsign, x) in self.callsign_minute_to_show) or (
                            confidence >= self.callsign_minute_to_show[(callsign, x)][2]
                        ):
                            self.callsign_minute_to_show[(callsign, x)] = (
                                show_name,
                                source,
                                confidence,
                            )

    def get_station_info(self, callsign):
        """Lookup station metadata for a specific callsign"""
        return self.station_metadata.get(callsign, None)

    def get_callsign_timezone(self, callsign):
        return self.callsign_to_timezone.get(callsign)

    @staticmethod
    def schedule_source_confidence(source):
        """Given a source code from the schedule data, return an ad hoc measure of how reliable
        it is.  This is used in tools that select a preferred airing of an episode to use when many
        are available."""
        if source.lower() in _SOURCE_CONFIDENCES:
            return _SOURCE_CONFIDENCES[source.lower()]
        elif source.startswith("auto_fill_"):
            return 1.0
        elif source.startswith("auto_correction"):
            return 0.40
        else:
            return 0.5

    def get_show_for_station_and_time(self, callsign, start_time):
        """Given a callsign and snippet starting datetime, return the name of the
        show that was being broadcast at that time, or None if not known."""
        timezone = self.callsign_to_timezone.get(callsign)
        if not timezone:
            return None
        local_tz = pytz.timezone(timezone)
        local_time = pytz.utc.localize(start_time).astimezone(local_tz)
        return self.get_show_for_station_and_local_time(
            callsign, local_time.weekday(), local_time.hour, local_time.minute
        )

    def get_show_for_station_and_local_time(self, callsign, weekday, hour, minute):
        start_minute = self._time_to_week_minute(weekday, hour, minute)
        return self.callsign_minute_to_show.get((callsign, start_minute))

    def station_to_metadata_dict(self):
        """Return a map of callsign -> station metadata object"""
        return self.station_metadata


def latlon_string_to_tuple(s):
    """Convert from radiolocator's string representation of latitude and longitude
    to a coordinate pair in degrees."""
    parts = [
        x.strip().replace("\u00b0", "").replace("'", "").replace('"', "")
        for x in s.split(",")
    ]
    latparts, lonparts = parts[0].split(), parts[1].split()
    d1, m1, s1 = [int(x) for x in latparts[:3]]
    lat = d1 + m1 / 60.0 + s1 / 3600.0
    d2, m2, s2 = [int(x) for x in lonparts[:3]]
    lon = d2 + m2 / 60.0 + s2 / 3600.0
    if latparts[-1] != "N":
        lat = -lat
    if lonparts[-1] != "E":
        lon = -lon
    return (lat, lon)


if __name__ == "__main__":
    x = StationMetadata(logger=logging.getLogger(__name__))
    print(x.get_station_info("KASC"))
