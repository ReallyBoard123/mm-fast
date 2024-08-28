import traceback
from .measurement_processing import * 
import pandas as pd  
import numpy as np
import logging
from pathlib import Path
import pytz
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)


def get_custom_handling_height_mapping(measurement_file_path):
    id_to_name_map, _ = get_handling_heights_plot_info(measurement_file_path)
    id_to_name_map[6] = "No Handle"  # or whatever is appropriate based on consultation
    return id_to_name_map

def get_multi_dim_id_series(*time_series, append_final_timestamp=True):
    diffs = [np.where(ts[:-1] != ts[1:])[0] + 1 for ts in time_series]
    timestamps = np.unique(np.concatenate(diffs))
    timestamps = np.insert(timestamps, 0, 0)
    id_series = list(zip(*[ts[timestamps] for ts in time_series]))
    
    if append_final_timestamp:
        timestamps = np.append(timestamps, len(time_series[0]))
    
    return id_series, timestamps

def time_from_iso_to_seconds(iso_str):
    dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6

def extract_measurement_data(data, selected_measurement_metadata, folder_path):
    try:
        set_id = selected_measurement_metadata['set_id']
        baseTime = selected_measurement_metadata['timestamp']
        
        # Convert UTC time to Germany time (CEST)
        utc_time = datetime.fromisoformat(baseTime.replace('Z', '+00:00'))
        germany_tz = pytz.timezone('Europe/Berlin')
        local_time = utc_time.astimezone(germany_tz)
        
        # Format the local time for the filename
        local_time_str = local_time.isoformat()

        base_seconds = time_from_iso_to_seconds(baseTime)

        region_uuid_to_name = {r['uuid']: r['name'] for r in data.processes[data.selected_process]["layout"]["regions"]}
        region_label_uuid_to_name = {r['uuid']: r['name'] for r in data.processes[data.selected_process]["layout"]["region_labels"]}

        fp = data.get_measurement_dir_path(data.selected_process, data.selected_measurement)

        region_timeseries = get_region_ts_for_measurement(fp)
        base_act_ts = get_base_activitiy_ts_for_measurement(fp)
        region_label_ts_data = get_region_label_ts_for_measurement(fp)
        step_ts = get_step_ts_for_measurement(fp)
        handling_heights_ts = get_handling_heights_ts_for_measurement(fp)

        base_act_id_to_name_map, _ = get_base_activity_plot_info(fp)
        handling_height_id_to_name_map = get_custom_handling_height_mapping(fp)

        id_series, timestamps = get_multi_dim_id_series(region_timeseries, base_act_ts, region_label_ts_data, step_ts, handling_heights_ts)

        start_timestamps = timestamps[:-1]
        end_timestamps = timestamps[1:]

        # Main sensor data
        sensor_df = pd.DataFrame({
            "Measurement ID": set_id,
            "Start Time(seconds)": start_timestamps / 100 + base_seconds,
            "End Time (seconds)": end_timestamps / 100 + base_seconds,
            "Region": [region_uuid_to_name.get(entry[0], "Unknown Region") for entry in id_series],
            "Activity": [base_act_id_to_name_map.get(entry[1], "Unknown Activity") for entry in id_series],
            "Region Label": [region_label_uuid_to_name.get(entry[2], "Unknown Label") for entry in id_series]
        })

        sensor_df["Duration"] = sensor_df["End Time (seconds)"] - sensor_df["Start Time(seconds)"]
        # Handling data
        handling_df = pd.DataFrame({
            "Measurement ID": set_id,
            "Start Time(seconds)": start_timestamps / 100 + base_seconds,
            "End Time (seconds)": end_timestamps / 100 + base_seconds,
            "Handling Height": [handling_height_id_to_name_map.get(entry[4], f"Unknown Height ({entry[4]})") for entry in id_series]
        })

        handling_df["Duration"] = handling_df["End Time (seconds)"] - handling_df["Start Time(seconds)"]
        

        # Save the original detailed CSV
        sensor_csv_file_path = Path(folder_path) / f"{set_id}_{local_time_str}_detailed.csv"
        sensor_df.to_csv(sensor_csv_file_path, index=False)

        handling_csv_path = Path(folder_path, f"{set_id}_{local_time_str}_handling.csv")
        handling_df.to_csv(handling_csv_path, index=False)
        
        # Create the summary DataFrame
        summary_df = create_summary_dataframe(sensor_df, local_time)

        # Save the summary CSV
        summary_csv_path = Path(folder_path) / f"{set_id}_{local_time_str}_summary.csv"
        summary_df.to_csv(summary_csv_path, index=False)

        print(f"Detailed data saved to {sensor_csv_file_path}")
        print(f"Summary data saved to {summary_csv_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

def create_summary_dataframe(df, start_time):
    def combine_rows(group):
        return pd.Series({
            'Start Time': group['Start Time'].iloc[0],
            'End Time': group['End Time'].iloc[-1],
            'Region': group['Region'].iloc[0],
            'Activity': group['Activity'].iloc[0],
            'Region Label': group['Region Label'].iloc[0],
            'Duration': (group['End Time'].iloc[-1] - group['Start Time'].iloc[0]).total_seconds()
        })

    # Convert seconds to datetime
    df['Start Time'] = start_time + pd.to_timedelta(df['Start Time(seconds)'] - df['Start Time(seconds)'].iloc[0], unit='s')
    df['End Time'] = start_time + pd.to_timedelta(df['End Time (seconds)'] - df['Start Time(seconds)'].iloc[0], unit='s')

    # Group by changes in Region or Activity
    grouped = df.groupby((df['Region'].shift() != df['Region']) | (df['Activity'].shift() != df['Activity'])).cumcount()
    summary_df = df.groupby(grouped).apply(combine_rows).reset_index(drop=True)

    return summary_df[['Start Time', 'End Time', 'Region', 'Activity', 'Region Label', 'Duration']]

def generate_beacon_summary(df):
    if df.empty:
        return pd.DataFrame()

    df['Duration'] = df['End Time (seconds)'] - df['Start Time(seconds)']
    summary = df.groupby(['Measurement ID', 'Beacon Closeness', 'Beacon Usage'])['Duration'].sum().reset_index()
    return summary

def extract_dynamic_beacon_data(data, selected_measurement_metadata, root_folder_path):
    try:
        set_id = selected_measurement_metadata['set_id']
        baseTime = selected_measurement_metadata['timestamp']
        base_seconds = time_from_iso_to_seconds(baseTime)

        date_folder_name = baseTime.split("T")[0]
        fp = Path(data.get_measurement_dir_path(data.selected_process, data.selected_measurement))

        if not fp.exists():
            logging.error(f"Measurement directory not found: {fp}")
            return

        try:
            region_timeseries = get_region_ts_for_measurement(fp)
            closeness_arr, usage_arr, beacon_uuids = get_dynamic_beacon_data_for_measurement(fp)
        except FileNotFoundError as e:
            logging.error(f"Required data file not found: {e}")
            return
        except Exception as e:
            logging.error(f"Error retrieving data: {str(e)}")
            return

        beacon_uuid_to_name = {beacon['uuid']: beacon['comment'] for beacon in data.processes[data.selected_process]["layout"]["dynamic_beacons"]}

        dynamic_beacon_dir = Path(root_folder_path) / date_folder_name / "dynamic_beacons"
        dynamic_beacon_dir.mkdir(parents=True, exist_ok=True)

        timestamps = np.arange(len(closeness_arr)) / 100 + base_seconds
        start_times = timestamps[:-1]
        end_times = timestamps[1:]

        for beacon_idx, beacon_uuid in enumerate(beacon_uuids):
            beacon_name = beacon_uuid_to_name.get(beacon_uuid, f"beacon_{beacon_uuid}")
            beacon_file_path = dynamic_beacon_dir / f"{beacon_name}.csv"
            beacon_summary_path = dynamic_beacon_dir / f"{beacon_name}_summary.csv"

            df = pd.DataFrame({
                "Start Time(seconds)": start_times,
                "End Time (seconds)": end_times,
                "Beacon Closeness": closeness_arr[:-1, beacon_idx],
                "Beacon Usage": usage_arr[:-1, beacon_idx],
                "Measurement ID": set_id
            })

            df = df[(df["Beacon Closeness"] | df["Beacon Usage"])]

            if not df.empty:
                # Save detailed data
                df.to_csv(beacon_file_path, mode='a', index=False, header=not beacon_file_path.exists())

                # Generate and save summary for this specific beacon
                summary_df = generate_beacon_summary(df)
                if not summary_df.empty:
                    summary_df.to_csv(beacon_summary_path, mode='a', index=False, header=not beacon_summary_path.exists())
                
                logging.info(f"Data and summary for beacon {beacon_name} saved successfully.")
            else:
                logging.info(f"No data for beacon {beacon_name}")

        logging.info(f"Data extraction and summary generation for dynamic beacons on {date_folder_name} completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred while extracting dynamic beacon data: {e}")
        traceback.print_exc()