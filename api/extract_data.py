import csv
import traceback
from .measurement_processing import *   
import numpy as np
import os

def get_multi_dim_id_series(*time_series, append_final_timestamp=True):
    diffs = []
    for ts in time_series:
        if not isinstance(ts, np.ndarray):
            ts = np.array(ts)
        if ts.dtype != object:
            diff = np.where(~((ts[:-1] == ts[1:]) | (np.isnan(ts[:-1]) & np.isnan(ts[1:]))))[0]
        else:
            diff = np.where(~(ts[:-1] == ts[1:]))[0]
        diffs.append(diff + 1)  # Adding 1 to align with the start of changes

    d_diffs = np.unique(np.concatenate(diffs))
    timestamps = np.insert(d_diffs, 0, 0)  # Ensure start is included
    id_series = list(zip(*[ts[timestamps] for ts in time_series]))

    if append_final_timestamp:
        timestamps = np.append(timestamps, len(time_series[0]))  # Ensure end is included

    return id_series, timestamps

def time_from_iso_to_seconds(iso_str):
    time_str = iso_str.split('T')[1].split('+')[0]  # Extract HH:MM:SS.ssssss
    h, m, s = map(float, time_str.split(':'))
    return int(h) * 3600 + int(m) * 60 + s

def extract_measurement_data(data, selected_measurement_metadata, folder_path):
    try:
        set_id = selected_measurement_metadata['set_id']
        baseTime = selected_measurement_metadata['timestamp']
        base_seconds = time_from_iso_to_seconds(selected_measurement_metadata['timestamp'])

        region_uuid_to_name = {r['uuid']: r['name'] for r in data.processes[data.selected_process]["layout"]["regions"]}
        region_label_uuid_to_name = {r['uuid']: r['name'] for r in data.processes[data.selected_process]["layout"]["region_labels"]}

        fp = data.get_measurement_dir_path(data.selected_process, data.selected_measurement)

        region_timeseries = get_region_ts_for_measurement(fp)
        base_act_ts = get_base_activitiy_ts_for_measurement(fp)
        region_label_ts_data = get_region_label_ts_for_measurement(fp)
        step_ts = get_step_ts_for_measurement(fp)
        handling_heights_ts = get_handling_heights_ts_for_measurement(fp)

        base_act_id_to_name_map, _ = get_base_activity_plot_info(fp)
        handling_height_id_to_name_map, _ = get_handling_heights_plot_info(fp)

        num_steps = int(np.sum(step_ts))

        id_series, timestamps = get_multi_dim_id_series(region_timeseries, base_act_ts, region_label_ts_data, step_ts, handling_heights_ts)

        start_timestamps = timestamps[:-1]  # Exclude the last timestamp for start times
        end_timestamps = timestamps[1:]  # Exclude the first timestamp for end times

        region_name_series = [region_uuid_to_name.get(entry[0], "Unknown Region") for entry in id_series]
        base_act_name_series = [base_act_id_to_name_map.get(entry[1], "Unknown Activity") for entry in id_series]
        region_label_series = [region_label_uuid_to_name.get(entry[2], "Unknown Label") for entry in id_series]
        handling_heights_series = [handling_height_id_to_name_map.get(entry[4], "Unknown Height") for entry in id_series]

        total_walking_duration = 0
        filtered_rows = []

        for i in range(len(id_series)):
            start_time_seconds = round((start_timestamps[i] / 100) + base_seconds, 3)
            end_time_seconds = round((end_timestamps[i] / 100) + base_seconds, 3)
            duration_seconds = end_time_seconds - start_time_seconds

            handling_height = handling_heights_series[i]

            row = (
                set_id, start_time_seconds, end_time_seconds, region_name_series[i],
                base_act_name_series[i], region_label_series[i], num_steps,
                handling_height
            )

            if base_act_name_series[i] == "Walk":
                total_walking_duration += duration_seconds

            filtered_rows.append(row)

        csv_file_path = os.path.join(folder_path, f"{set_id}_{baseTime}.csv")
        with open(csv_file_path, "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([
                "Measurement ID", "Start Time(seconds)", "End Time (seconds)", "Region",
                "Activity", "Region Label", "Total Steps", "Handling Height"
            ])
            for row in filtered_rows:
                csv_writer.writerow(row)

        print(f"Data saved to {csv_file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
