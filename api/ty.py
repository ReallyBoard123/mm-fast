import os
import gzip
import pickle
import numpy as np

from .translations import ACTIVITY_TRANSLATION_KEY_TO_NAME
from .exceptions import EmptyTimeSeriesException


def get_id_series(uuid_timeseries, append_final_timestamp=False):
    
def get_time_series_for_id_series(id_series, timestamps):


def get_pause_ts_for_measurement(measurement_file_path):
    
    file_path = os.path.join(measurement_file_path, 'pause_data.pickle')
    with open(file_path, 'rb') as fd:
        data_container = pickle.load(fd)
        data = data_container['data']
        id_series = data['id_series']
        timestamps = data['timestamps']
        ts = get_time_series_for_id_series(id_series, timestamps)
        return ts


def get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=True):
    
    file_path = os.path.join(measurement_file_path, target_ts_name)
    with open(file_path, 'rb') as fd:
        data_container = pickle.load(fd)
        data = data_container['data']
        id_series = data['id_series']
        timestamps = data['timestamps']
        ts = get_time_series_for_id_series(id_series, timestamps)
        if remove_pauses:
            pause_ts = get_pause_ts_for_measurement(measurement_file_path)
            # Shorten to min length
            min_length = min(pause_ts.shape[0], ts.shape[0])
            ts = ts[:min_length]
            pause_ts = pause_ts[:min_length]
            # Keep entries where pause is not True
            ts = ts[~pause_ts]
        return ts


def get_region_ts_for_measurement(measurement_file_path, remove_pauses=True):
   
    target_ts_name = 'region_ts_data.pickle'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_region_label_ts_for_measurement(measurement_file_path, remove_pauses=True):
   
    target_ts_name = 'region_label_ts_data.pickle'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_base_activitiy_ts_for_measurement(measurement_file_path, remove_pauses=True):

def get_base_activity_plot_info(measurement_file_path):
   
def get_handling_heights_ts_for_measurement(measurement_file_path, remove_pauses=True):
    



def get_handling_heights_plot_info(measurement_file_path):


def get_step_ts_for_measurement(measurement_file_path, remove_pauses=True):
    
    target_ts_name = 'step_counter_data.pickle'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_walking_speed_ts_for_measurement(measurement_file_path, remove_pauses=True):
    
    target_ts_name = 'walking_speed_data.pickle'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_shift_data_for_measurement(measurement_file_path, remove_pauses=True):
    
    file_path = os.path.join(measurement_file_path, 'shift_data.pickle.gz')
    with gzip.open(file_path, 'rb') as fd:
        data_container = pickle.load(fd)
        data = data_container['data']
        shift_arr = data['shift_arr']
        shift_pause_arr = data['shift_pause_arr']
        shift_uuids = data['uuids']
        if remove_pauses:
            pause_ts = get_pause_ts_for_measurement(measurement_file_path)
            # Shorten to min length
            min_length = min(pause_ts.shape[0], shift_arr.shape[0])
            shift_arr = shift_arr[:min_length, :]
            shift_pause_arr = shift_pause_arr[:min_length, :]
            pause_ts = pause_ts[:min_length]
            # Keep entries where pause is not True
            shift_arr = shift_arr[~pause_ts, :]
            shift_pause_arr = shift_pause_arr[~pause_ts, :]
        return shift_arr, shift_pause_arr, shift_uuids


def get_hour_of_day_ts_for_measurement(measurement_file_path, remove_pauses=True):
    
    target_ts_name = 'hour_of_day_data.pickle'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_date_ts_for_measurement(measurement_file_path, remove_pauses=True):
    
    target_ts_name = 'date_data.pickle'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_dynamic_beacon_data_for_measurement(measurement_file_path, remove_pauses=True):
    
    file_path = os.path.join(measurement_file_path, 'dyn_beacon_usage_closeness_data.pickle.gz')
    with gzip.open(file_path, 'rb') as fd:
        data_container = pickle.load(fd)
        data = data_container['data']
        closeness_arr = data['closeness_arr']
        usage_arr = data['usage_arr']
        uuids = data['uuids']
        if remove_pauses:
            pause_ts = get_pause_ts_for_measurement(measurement_file_path)
            # Shorten to min length
            min_length = min(pause_ts.shape[0], closeness_arr.shape[0])
            closeness_arr = closeness_arr[:min_length, :]
            usage_arr = usage_arr[:min_length, :]
            pause_ts = pause_ts[:min_length]
            # Keep entries where pause is not True
            closeness_arr = closeness_arr[~pause_ts, :]
            usage_arr = usage_arr[~pause_ts, :]
        return closeness_arr, usage_arr, uuids
