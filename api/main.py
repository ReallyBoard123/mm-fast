from .extract_data import extract_measurement_data, extract_dynamic_beacon_data
from .data import MMLabsData
import pandas as pd
import os
import traceback
import io
from contextlib import redirect_stdout

def setup_api(access_token, data_dir):
    try:
        data = MMLabsData(data_dir=data_dir, offline_mode=False)
        data.add_api_token(access_token)
        return data 
    except Exception as e:
        print(f"Error setting up API: {e}")
        traceback.print_exc()

def list_processes(data):
    try:
        return pd.DataFrame(data.processes.keys(), columns=["process_uuid"])
    except Exception as e:
        print(f"Error listing processes: {e}")
        traceback.print_exc()

def download_data_for_process(data, process_uuid):
    try:
        data.get_api_all_data(process_uuid)
        data.selected_process = process_uuid
        print(f"Data for process {process_uuid} downloaded and set for later use.")
    except Exception as e:
        print(f"Error downloading data for process {process_uuid}: {e}")
        traceback.print_exc()

def list_measurements(data, process_uuid):
    try:
        measurements = data.get_api_measurements(process_uuid)
        return pd.DataFrame.from_dict(measurements).set_index('timestamp').sort_index(ascending=False)
    except Exception as e:
        print(f"Error listing measurements for process {process_uuid}: {e}")
        traceback.print_exc()
        return None

def extract_data(data, process_uuid, root_folder_path):
    logs = io.StringIO()
    measurements_df = list_measurements(data, process_uuid)
    
    if measurements_df is not None:
        with redirect_stdout(logs):
            for measurement in measurements_df.itertuples():
                data.selected_measurement = measurement.uuid
                try:
                    selected_measurement_metadata = next(
                        m for m in data.get_api_measurements(process_uuid) 
                        if m["uuid"] == data.selected_measurement
                    )
                    
                    measurement_date = selected_measurement_metadata['timestamp'][:10]
                    sensor_set_id = selected_measurement_metadata['set_id']
                    
                    measurement_folder_path = os.path.join(
                        root_folder_path, measurement_date, sensor_set_id
                    )
                    os.makedirs(measurement_folder_path, exist_ok=True)
                    
                    print(f"Processing measurement {sensor_set_id} on {measurement_date}...")

                    # Extract raw sensor and handling data
                    extract_measurement_data(data, selected_measurement_metadata, measurement_folder_path)
                    
                    # Extract dynamic beacon data
                    extract_dynamic_beacon_data(data, selected_measurement_metadata, os.path.join(root_folder_path, measurement_date))
                    
                except Exception as e:
                    print(f"Error extracting data for measurement {measurement.uuid}: {e}")
                    traceback.print_exc()
    
    return logs.getvalue().split('\n')