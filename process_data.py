#!/usr/bin/env python3
import pandas as pd
import re
import sys
import argparse
import os
import functools


def time_to_parts(time):
    time_parts = [int(x) for x in time.split(":")]
    return time_parts[0] * 3600 + time_parts[1] * 60 + time_parts[2]


def compute_diff(t1, t2):
    time1 = time_to_parts(t1)
    time2 = time_to_parts(t2)
    return time2 - time1


def process_name(st_name):
    return re.sub(r" - [ABC]", "", st_name)


def compute_all_diferences(arrival_times, departure_times):
    return [compute_diff(first, second)
            for first, second in zip(arrival_times, departure_times)]


def create_csv(joined, output_path, suffix, stops):
    grouped = joined.groupby("trip_id", sort=True)
    data = []
    for group in grouped:
        g = group[1].sort_values(by="stop_sequence")
        stations = ["->".join(x) for x in zip(g.stop_name.apply(process_name),
                    g.stop_name[1:].apply(process_name))]
        times = compute_all_diferences(g.arrival_time, g.arrival_time[1:])
        data += [{"section": x[0], "time": x[1]} for x in zip(stations, times)]
    df = pd.DataFrame.from_records(data)
    grouped = df.groupby("section").mean()
    df2 = grouped.copy()
    sections = grouped.index.map(lambda x: x.split("->"))
    df2["departure_station"] = [x[0] for x in sections]
    df2["arrival_station"] = [x[1] for x in sections]
    df2.to_csv(os.path.join(output_path, "times{0}.csv".format(suffix)))
    stops2 = stops.copy()
    stops2["stop_name"] = stops2.stop_name.apply(lambda x: process_name(x)[0])
    stops_grouped = stops2.groupby("stop_name").mean()
    stops_grouped[["stop_lat", "stop_lon"]].apply(lambda x: x.round(6)).to_csv(os.path.join(output_path, "locations{0}.csv".format(suffix)))


def parse_arguments(argv):
    parser = argparse.ArgumentParser(description='Process Prague data')
    parser.add_argument('data_path', type=str,
                        help="Path to the folder containing GTFS files")
    parser.add_argument("output_path", type=str,
                        help="Path to output folder.")
    return parser.parse_args(argv)


def main(argv):
    args = parse_arguments(argv)
    stops_cols = ["stop_id", "stop_lat", "stop_lon", "stop_name"]
    stops = pd.read_csv(os.path.join(args.data_path, "stops.txt"))[stops_cols]
    stop_times = pd.read_csv(os.path.join(args.data_path,
                                          "stop_times.txt"))
    trips = pd.read_csv(os.path.join(args.data_path, "trips.txt"))
    calendar = pd.read_csv(os.path.join(args.data_path, "calendar.txt"))
    routes = pd.read_csv(os.path.join(args.data_path, "routes.txt"))
    joined = functools.reduce(lambda x, y: x.merge(y[0], on=y[1]),
                              [(stop_times, "trip_id"),
                              (calendar, "service_id"), (stops, "stop_id"),
                              (routes, "route_id")], trips)
    print("Data loaded")
    night_start = time_to_parts("00:00:00")
    night_end = time_to_parts("05:00:00")

    joined["arrival_time_seconds"] = joined.arrival_time.apply(time_to_parts)
    joined["departure_time_seconds"] = joined.departure_time.apply(time_to_parts)

    night_time = joined[(joined.departure_time_seconds >= night_start) &
                        (joined.departure_time_seconds < night_end)]
    day_time = joined[joined.departure_time_seconds >= night_end]

    create_csv(night_time, args.output_path, "_night", stops)
    print("Created night data.")
    create_csv(day_time, args.output_path, "_day", stops)
    print("Created day data.")

    # joined = trips.merge(stop_times, on="trip_id").
    #             merge(calendar, on="service_id").merge(stops, on="stop_id").
    #             merge(routes, on="route_id")

if __name__ == "__main__":
    main(sys.argv[1:])
