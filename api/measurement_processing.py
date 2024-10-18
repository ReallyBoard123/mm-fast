"""
@created: 13.07.2021
@copyright: Motion Miners GmbH, Emil-Figge Str. 80, 44227 Dortmund, 2021

@brief: Functions for loading and processing data of downloaded measurement files
"""

import os
import json
import h5py
import numpy as np
from pathlib import Path 

from .translations import ACTIVITY_TRANSLATION_KEY_TO_NAME
from .exceptions import EmptyTimeSeriesException

def __hdf5_to_dict(hdf5obj: h5py.File) -> dict: 
    """
    Recursively converts h5py.File object to Python dictionary.
    """
    def __recursive_helper(obj):
        if isinstance(obj, h5py.Group):
            return {key: __recursive_helper(obj[key]) for key in obj}
        elif isinstance(obj, h5py.Dataset):
            if isinstance(obj[()], bytes):
                return obj[()].decode()
            else:
                return obj[()]
        else:
            return obj

    return __recursive_helper(hdf5obj)
    

def get_available_ts_for_measurement(measurement_file_path: str) -> list[str] :
    """
      Get the available types of timeseries data for a specified measurement file. 
      This method attempts to read the metadata for the process found as the parent
      of the measurement file path. 

      Note: Due to legacy reasons, the metadata provided contains a `.pickle` postfix.
        This function removes that file ending, as it is no longer accurate.

      :param measurement_file_path: The file path for the measurement to inspect.

      :return: A list of strings with available timeseries for the measurement.
    """

    # given the structure of the data directory, parent_file_path is similar to the process UUID
    parent_file_path = Path(measurement_file_path).parent
    with parent_file_path.joinpath('process_metadata.json').open() as json_file:
        data = json.load(json_file)
    
    available_ts = data['available_measurement_files']
    # Use map to remove the ".pickle" ending from each string in the list
    return list(map(lambda x: x[:-7] if x.endswith('.pickle') else x, available_ts))

def load_timeseries(measurement_file_path: str, target_ts_name: str, remove_pauses: bool = True) -> None:
    """
      Load an id series from the provided HDF5 file.

      :param measurement_file_path: The fully qualified file path, including the process and measurement UUIDs
      :param target_ts_name: The name of the time series file to be loaded. 
      :param remove_pauses: Boolean to indicate whether or not to remove pause durations from the result 

      :return: The requested time series data as `np.ndarray`
    """

    available_ts: list[str] = get_available_ts_for_measurement(measurement_file_path)
    if target_ts_name not in available_ts:
        raise Exception(f"{target_ts_name} is not included in measurement ${measurement_file_path}")
    
    file_path = os.path.join(measurement_file_path, target_ts_name)
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
        id_series, timestamps = data['id_series'], data['timestamps']
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
    

def get_id_series(uuid_timeseries, append_final_timestamp=False) -> (np.ndarray, np.ndarray): # type: ignore
    """
    Takes a discrete time series and computes a compressed id series denoting changes in ids.
    Returns an id series and corresponding start timestamps
    i.e. 1 1 1 2 2 -> [1, 2], [0, 3]
    with [1, 2] being the ids and [0, 3] the start timestamps

    :param uuid_timeseries: discrete time series of ids
    :param append_final_timestamp: appends a final timestamp, which equals the length of the timeseries
    
    :return: id_series, timestamps
    """
    # Make sure we operate on a numpy array
    if not isinstance(uuid_timeseries, np.ndarray):
        uuid_timeseries = np.array(uuid_timeseries)
    # Make sure array is not empty
    if uuid_timeseries.shape[0] == 0:
        raise EmptyTimeSeriesException(
            'Tried to get id series from empty time series')

    # Start with timestamp 0 and afterwards check for changes between adjacent values in the timeseries
    timestamps = np.append([0], np.where(
        uuid_timeseries[:-1] != uuid_timeseries[1:])[0])
    # Need to increase timestamps by 1 because we calculated the changes starting from second index
    timestamps[1:] = timestamps[1:] + 1
    id_series = uuid_timeseries[timestamps]

    # Append final timestamp
    if append_final_timestamp:
        timestamps = np.append(timestamps, uuid_timeseries.shape[0])

    return id_series, timestamps


def get_time_series_for_id_series(id_series, timestamps):
    """
    Inverse operation of get_id_series.
    We assume that the final timestamp, which equals the length of the timeseries was appended to timestamps.
    Return the discrete series of ids.

    :param id_series: compressed id series
    :param timestamps: start timestamps with length of the uncompressed timeseries appended
    :return: id_timeseries
    """
    # Make sure we operate on a numpy array
    if not isinstance(id_series, np.ndarray):
        id_series = np.array(id_series)
    if not isinstance(timestamps, np.ndarray):
        timestamps = np.array(timestamps)

    # Edge case empty arrays
    if timestamps.shape[0] == 0 or id_series.shape[0] == 0:
        return np.array([])

    # Build id series
    id_timeseries = np.empty(timestamps[-1], dtype=id_series.dtype)
    for i, _ in enumerate(timestamps):
        id_timeseries[timestamps[i - 1]: timestamps[i]] = id_series[i - 1]

    return id_timeseries


def get_pause_ts_for_measurement(measurement_file_path):
    """
    Returns the pause time series for the given measurement in form of [False, False, False, True, True, ...]
    Pauses can be the result of longer periods without any sensor movement or stays in regions excluded from evaluation.
    It is generally recommended to remove (slice) pauses from all processed measurements.

    :param measurement_file_path
    :return: pause time series for the given measurement in form of [False, False, False, True, True, ...]
    """
    file_path = os.path.join(measurement_file_path, 'pause_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
        id_series = data['id_series']
        timestamps = data['timestamps']
        ts = get_time_series_for_id_series(id_series, timestamps)
        return ts


def get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=True):
    """
    Loads id series from pickle file and return resampled id series

    :param measurement_file_path 
    :paran target_ts_name 
    :param remove_pauses
    :return: resampled id series
    """
    file_path = os.path.join(measurement_file_path, target_ts_name)
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
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


def get_region_ts_for_measurement(measurement_file_path, remove_pauses=True) -> np.ndarray[str]:
    """
    Returns the region ts in form of [<region_uuid>] for the given measurement.
    The time series is resampled to a resolution of 10ms (100 entries == 1 second).
    By default also removes (slices parts) pauses.
    The unknown region is represented by uuid 'None'.

    :param measurement_file_path 
    :param remove_pauses
    :return: the region ts in form of [<region_uuid>] for the given measurement
    """
    target_ts_name = 'region_ts_data'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses).astype(str)


def get_region_label_ts_for_measurement(measurement_file_path, remove_pauses=True) -> np.ndarray[str]:
    """
    Returns the region label ts in form of [<region_label_uuid>] for the given measurement.
    The time series is resampled to a resolution of 10ms (100 entries == 1 second).
    By default also removes (slices parts) pauses.
    The unknown region label is represented by uuid 'None'. 
    This means either a stay in a region without an assigned label or unknown region

    :param measurement_file_path 
    :param remove_pauses
    :return: the region label ts in form of [<region_label_uuid>] for the given measurement
    """
    target_ts_name = 'region_label_ts_data'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses).astype(str)


def get_base_activitiy_ts_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns the activity time series in form of [<activity_id>] for the given measurement.
    The time series is resampled to a resolution of 10ms (100 entries == 1 second).
    By default also removes (slices parts) pauses.

    :param measurement_file_path 
    :param remove_pauses
    :return: activity time series in form of [<activity_id>] for the given measurement
    """
    file_path = os.path.join(measurement_file_path, 'unified_har_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
        base_act_data = data['1']['act_id_ts']
        id_series = base_act_data['id_series']
        timestamps = base_act_data['timestamps']
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


def get_base_activity_plot_info(measurement_file_path):
    """
    Returns id to human readable name mapping and preferred chart color mapping
    for base activities of the given measurement.
    Result can be assumed to be constant across all measurements of a process.

    :param measurement_file_path 
    :return: id to human readable name mapping and preferred chart color data
    for base activities of the given measurement
    """
    file_path = os.path.join(measurement_file_path, 'unified_har_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
        available_acts = data['1']['available_acts']

        id_to_name_map = {
                available_acts[act]['id']: ACTIVITY_TRANSLATION_KEY_TO_NAME[available_acts[act]['translation_key']] for act in available_acts}   
        id_to_color_map = {available_acts[act]['id']: available_acts[act]['chart_color']
                           for act in available_acts}
        return id_to_name_map, id_to_color_map


def get_handling_heights_ts_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns the handling heights time series in form of [<handling_height_id>] for the given measurement.
    The time series is resampled to a resolution of 10ms (100 entries == 1 second).
    By default also removes (slices parts) pauses.

    :param measurement_file_path 
    :param remove_pauses
    :return: handling heights time series in form of [<handling_height_id>] for the given measurement
    """
    file_path = os.path.join(measurement_file_path, 'unified_har_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
        base_act_data = data['2']['act_id_ts']
        id_series = base_act_data['id_series']
        timestamps = base_act_data['timestamps']
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


def get_handling_heights_plot_info(measurement_file_path):
    """
    Returns id to human readable name mapping and preferred chart color mapping
    for handling heights of the given measurement.
    Result can be assumed to be constant across all measurements of a process.

    :param measurement_file_path 
    :return: id to human readable name mapping and preferred chart color data
    for handling heights of the given measurement
    """
    file_path = os.path.join(measurement_file_path, 'unified_har_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
        available_acts = data['2']['available_acts']       
        id_to_name_map = {
                available_acts[act]['id']: ACTIVITY_TRANSLATION_KEY_TO_NAME[available_acts[act]['translation_key']] for act in available_acts}   
        id_to_color_map = {available_acts[act]['id']: available_acts[act]['chart_color']
                           for act in available_acts}
        return id_to_name_map, id_to_color_map


def get_step_ts_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns the step ts in form of [0, 0, 0, 1, 0, 0, 1, 0, ...] for the given measurement.
    Entries with value 1 indicate a step at the index corresponding to a specific point in time of the measurement.
    The time series is resampled to a resolution of 10ms (100 entries == 1 second).
    By default also removes pauses by slicing parts of the ts.

    :param measurement_file_path 
    :param remove_pauses
    :return: step ts in form of [0, 0, 0, 1, 0, 0, 1, 0, ...] for the given measurement
    """
    target_ts_name = 'step_counter_data'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_walking_speed_ts_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns the walking speed in form of [0.0, 0.0, 0.0, 4.7, 4.7, 4,7, 4,7, 4,7, ...] for the given measurement.
    Entries correspond to the walking speed in m/s for each walking segment longer than 5s.
    The distance per step is approximated at 0.7m (average male step length)-
    The time series is resampled to a resolution of 10ms (100 entries == 1 second).
    By default also removes pauses by slicing parts of the ts.

    :param measurement_file_path 
    :param remove_pauses
    :return: walking speed in form of [0.0, 0.0, 0.0, 4.7, 4.7, 4,7, 4,7, 4,7, ...] for the given measurement
    """
    target_ts_name = 'walking_speed_data'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_shift_data_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns data to check which point in time of the measurement matched shifts or shift pauses
    The shifts can be inspected via the process metadata. At least one default shift is always present.
    Return values are:   
      - shift_arr: np.ndarray(np.bool), timestamps x shifts, True if timestamp of measurements belongs to shift
      - shift_pause_arr: np.ndarray(np.bool), timestamps x shifts, 
        True if timestamp of measurements belongs to any shift pause of this shift
      - shift_uuids: np.ndarray(<U36, unicode string), UUIDs of shifts to identify correct idx in arr

    :param measurement_file_path 
    :param remove_pauses
    :return: shift_arr, shift_pause_arr, shift_uuids
    """
    file_path = os.path.join(measurement_file_path, 'shift_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
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
    """
    Returns the hour of day time series in form of [11, 11, 11, 12, 12, 12, ...] for the given measurement.
    Useful to still be able to determine the hour of day for chart creation/filtering after different slicing operation.
    By default also removes pauses by slicing parts of the ts.
    Hours are stated in locale time matching the layout coordinates.

    :param measurement_file_path 
    :param remove_pauses
    :return: hour of day time series in form of [11, 11, 11, 12, 12, 12, ...] for the given measurement
    """
    target_ts_name = 'hour_of_day_data'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_date_ts_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns the date time series in form of [2021-12-19, 2021-12-19, ...] for the given measurement.
    Entries are of type datetime.date.
    If the measurement does not contain midnight the date is the same for each timestamp.
    Useful to still be able to determine the day for chart creation/filtering after different slicing operation.
    By default also removes pauses by slicing parts of the ts.

    :param measurement_file_path 
    :param remove_pauses
    :return: date time series in form of [2021-12-19, 2021-12-19, ...] for the given measurement
    """
    target_ts_name = 'date_data'
    return get_simple_ts_from_pickle_file(measurement_file_path, target_ts_name, remove_pauses=remove_pauses)


def get_dynamic_beacon_data_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns beacon type, closeness and usage info for all dynamic beacons
    Return values are:   
      - closeness_arr: np.ndarray(np.int32), timestamps x beacons x beacon_is_nearby
      - usage_arr: np.ndarray(np.int32), timestamps x beacons x beacon_is_used
      - uuids: np.ndarray(np.uint32), UUIDs of beacons to identify correct idx in arr

    :param measurement_file_path 
    :param remove_pauses
    :return: closeness_arr, usage_arr, uuids
    """
    file_path = os.path.join(measurement_file_path,
                             'dyn_beacon_usage_closeness_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
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


def get_fork_height_data_for_measurement(measurement_file_path, remove_pauses=True):
    """
    Returns fork height level, fork movement and target fork height level timeseries data for all dynamic beacons that are attached to forks of fork lift vehicles. Every level change begins and ends at level 0. Therefore, level changes from e.g. level 1 to level 2 won't be detected.
        So it is possible to detect the following TS [0,1,0,2,0,3,0 ], but not [0,1,2,1,0].
    Return value are:
      - fork_lvl_arr: np.ndarray(np.int32), timestamps x beacons x current_level_of_the_beacon
      - fork_target_lvl_arr: np.ndarray(np.int32), timestamps x beacons x indicates the final movement level a lift movement is going to reach for the whole movement duration (fork_lvl_time + time the movement to the level and back down took)
      - beacon_uuids: np.ndarray(np.uint32), UUIDs of beacons to identify correct idx in arr
      - beacon_type_ids: np.ndarray(np.uint32), type ids of the respective beacon uuids
      - target_heights: np.ndarray(np.uint32), height in meters of the fork lvls, can be used to map from lvl to actual height

    :param measurement_file_path
    :param remove_pauses
    :return: fork_lvl_arr, fork_target_lvl_arr, beacon_uuids, beacon_type_ids, target_heights
    """
    file_path = os.path.join(measurement_file_path,
                             'fork_height_lvl_data')
    with h5py.File(file_path, 'r') as data_container:
        data = __hdf5_to_dict(data_container['data'])
        fork_lvl_arr = data["lvl_arr"]
        fork_target_lvl_arr = data["lvl_movement_arr"]
        beacon_uuids = data["uuids"]
        beacon_type_ids = data["beacon_type_ids"]
        target_heights = data["target_heights"]
        if remove_pauses:
            # Shorten to min length
            pause_ts = get_pause_ts_for_measurement(measurement_file_path)
            min_length = min(pause_ts.shape[0], fork_lvl_arr.shape[0])
            fork_lvl_arr = fork_lvl_arr[:min_length, :]
            fork_target_lvl_arr = fork_target_lvl_arr[:min_length, :]
            # Keep entries where pause is not True
            fork_lvl_arr = fork_lvl_arr[~pause_ts, :]
            fork_target_lvl_arr = fork_target_lvl_arr[~pause_ts, :]

    return fork_lvl_arr,  fork_target_lvl_arr, beacon_uuids, beacon_type_ids, target_heights
