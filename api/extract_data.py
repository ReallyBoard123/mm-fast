import traceback
from .measurement_processing import *
import pandas as pd
import numpy as np
import logging
from pathlib import Path
import pytz
from datetime import datetime, timedelta
import pickle
import gzip
import os
from decimal import Decimal, ROUND_HALF_UP

logging.basicConfig(level=logging.INFO)

ACTIVITY_TRANSLATION_KEY_TO_NAME = {
    'HUMAN_ACT.WALK': 'Walk',
    'HUMAN_ACT.OTHER': 'Unknown',
    'HUMAN_ACT.STAND': 'Stand',
    'HUMAN_ACT.HANDLE': 'Handling',
    'HUMAN_ACT.DRIVE': 'Drive',
    'HUMAN_ACT.NO_HANDLE': 'No Handling',
    'HUMAN_ACT.HANDLE_UP': 'Handle up',
    'HUMAN_ACT.HANDLE_CENTER': 'Handle center',
    'HUMAN_ACT.HANDLE_DOWN': 'Handle down'
}

def load_pickle(file_path):
    with open(file_path, 'rb') as f:
        return pickle.load(f)

def load_gzip_pickle(file_path):
    with gzip.open(file_path, 'rb') as f:
        return pickle.load(f)

def time_from_iso_to_seconds(iso_str):
    dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6

def round_to_3_decimals(value):
    return Decimal(value).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)

def get_custom_handling_height_mapping(measurement_file_path):
    id_to_name_map, _ = get_handling_heights_plot_info(measurement_file_path)
    id_to_name_map[6] = "No Handle"  # or whatever is appropriate based on consultation
    return id_to_name_map

def process_measurement_data(measurement_file_path, process_metadata, base_seconds):
    date_data = load_pickle(os.path.join(measurement_file_path, 'date_data.pickle'))
    hour_data = load_pickle(os.path.join(measurement_file_path, 'hour_of_day_data.pickle'))
    region_data = load_pickle(os.path.join(measurement_file_path, 'region_ts_data.pickle'))
    activity_data = load_pickle(os.path.join(measurement_file_path, 'unified_har_data.pickle'))
    shift_data = load_gzip_pickle(os.path.join(measurement_file_path, 'shift_data.pickle.gz'))
    handling_heights_data = activity_data

    start_date = date_data['data']['id_series'][0]
    base_date = datetime.combine(start_date, datetime.min.time())

    region_ts = region_data['data']['id_series']
    region_timestamps = region_data['data']['timestamps']

    activity_ts = activity_data['data'][1]['act_id_ts']['id_series']
    activity_timestamps = activity_data['data'][1]['act_id_ts']['timestamps']

    shift_arr = shift_data['data']['shift_arr']
    shift_uuids = shift_data['data']['uuids']

    hour_ts = hour_data['data']['id_series']
    hour_timestamps = hour_data['data']['timestamps']

    handling_heights_ts = handling_heights_data['data'][2]['act_id_ts']['id_series']
    handling_heights_timestamps = handling_heights_data['data'][2]['act_id_ts']['timestamps']

    df = pd.DataFrame({
        'timestamp': [base_date + timedelta(seconds=t/100) for t in range(len(shift_arr))],
        'hour': np.repeat(hour_ts, np.diff(hour_timestamps)),
        'shift': [shift_uuids[np.where(row)[0][0]] if np.any(row) else None for row in shift_arr],
        'region': np.repeat(region_ts, np.diff(region_timestamps)),
        'activity': np.repeat(activity_ts, np.diff(activity_timestamps)),
        'handling_height': np.repeat(handling_heights_ts, np.diff(handling_heights_timestamps))
    })

    region_uuid_to_name = {r['uuid']: r['name'] for r in process_metadata['layout']['regions']}
    df['region_name'] = df['region'].map(region_uuid_to_name).fillna('Unknown Region')

    activity_id_to_key = {act['id']: act['translation_key'] for act in activity_data['data'][1]['available_acts']}
    df['activity_name'] = df['activity'].map(activity_id_to_key).map(ACTIVITY_TRANSLATION_KEY_TO_NAME)

    handling_height_mapping = get_custom_handling_height_mapping(measurement_file_path)
    df['handling_height_name'] = df['handling_height'].map(handling_height_mapping)

    df['Start Time'] = df['timestamp'].dt.hour * 3600 + df['timestamp'].dt.minute * 60 + df['timestamp'].dt.second + df['timestamp'].dt.microsecond / 1e6 + base_seconds
    df['End Time'] = df['Start Time'] + 0.01

    df['Start Time'] = df['Start Time'].apply(round_to_3_decimals)
    df['End Time'] = df['End Time'].apply(round_to_3_decimals)

    return df

def extract_measurement_data(data, selected_measurement_metadata, folder_path):
    try:
        set_id = selected_measurement_metadata['set_id']
        baseTime = selected_measurement_metadata['timestamp']
        
        utc_time = datetime.fromisoformat(baseTime.replace('Z', '+00:00'))
        germany_tz = pytz.timezone('Europe/Berlin')
        local_time = utc_time.astimezone(germany_tz)
        
        local_time_str = local_time.strftime("%Y-%m-%dT%H_%M_%S.%f%z")

        base_seconds = time_from_iso_to_seconds(baseTime)

        fp = data.get_measurement_dir_path(data.selected_process, data.selected_measurement)
        
        df = process_measurement_data(fp, data.processes[data.selected_process], base_seconds)

        # Main sensor data
        sensor_df = df[['Start Time', 'End Time', 'region_name', 'activity_name']].copy()
        sensor_df.columns = ['Start Time', 'End Time', 'Region', 'Activity']
        sensor_df['Set ID'] = set_id
        sensor_df = sensor_df[['Set ID', 'Start Time', 'End Time', 'Region', 'Activity']]

        # Handling data
        handling_df = df[['Start Time', 'End Time', 'region_name', 'handling_height_name']].copy()
        handling_df.columns = ['Start Time', 'End Time', 'Region', 'Handle Position']
        handling_df['Set ID'] = set_id
        handling_df = handling_df[['Set ID', 'Handle Position', 'Start Time', 'End Time', 'Region']]

        # Save the detailed CSVs
        sensor_csv_file_path = Path(folder_path) / f"{set_id}_{local_time_str}_sensor_raw.csv"
        sensor_df.to_csv(sensor_csv_file_path, index=False)

        handling_csv_path = Path(folder_path) / f"{set_id}_{local_time_str}_handling_raw.csv"
        handling_df.to_csv(handling_csv_path, index=False)

        print(f"Sensor raw data saved to {sensor_csv_file_path}")
        print(f"Handling raw data saved to {handling_csv_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

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