from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import os
import tempfile
import shutil
import uuid
import zipfile
import io
import pandas as pd
from api.data import MMLabsData
from api.exceptions import MMLabsException
from api.measurement_processing import *
import logging
from datetime import datetime, timedelta
import pytz

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://nextjs-frontend-b6w0.onrender.com", "http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TokenRequest(BaseModel):
    token: str

TEMP_DOWNLOAD_DIR = tempfile.mkdtemp()
logger.debug(f"Temporary download directory: {TEMP_DOWNLOAD_DIR}")

@app.post("/api/validate-and-process")
async def validate_and_process(request: TokenRequest):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.debug(f"Created temporary directory: {temp_dir}")
            data = MMLabsData(data_dir=temp_dir, offline_mode=False)
            data.add_api_token(request.token)
            
            process_uuid = next(iter(data.processes.keys()))
            logger.debug(f"Processing UUID: {process_uuid}")
            data.get_api_all_data(process_uuid)
            
            processed_data_dir = os.path.join(temp_dir, "processed_data")
            os.makedirs(processed_data_dir, exist_ok=True)
            logger.debug(f"Created processed data directory: {processed_data_dir}")
            
            logs = process_data(data, process_uuid, processed_data_dir)
            
            zip_filename = f"{uuid.uuid4()}.zip"
            zip_path = os.path.join(TEMP_DOWNLOAD_DIR, zip_filename)
            logger.debug(f"Creating zip file: {zip_path}")
            
            shutil.make_archive(zip_path[:-4], 'zip', processed_data_dir)
            logger.debug(f"Zip file created: {os.path.exists(zip_path)}")
            
            download_link = f"/api/download/{zip_filename}"
            
            return {"valid": True, "message": "Token is valid and data processed", "download_link": download_link, "logs": logs}
    except MMLabsException as e:
        logger.error(f"MMLabsException: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

@app.post("/api/process-data")
async def process_uploaded_data(file: UploadFile = File(...)):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(io.BytesIO(await file.read()), 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            processed_data_dir = os.path.join(temp_dir, "processed_data")
            os.makedirs(processed_data_dir, exist_ok=True)

            data = MMLabsData(data_dir=temp_dir, offline_mode=True)
            data.read_cache()

            logs = []
            for process_uuid in data.processes.keys():
                logs.extend(process_data(data, process_uuid, processed_data_dir))

            processed_zip_filename = f"processed_{uuid.uuid4()}.zip"
            processed_zip_path = os.path.join(TEMP_DOWNLOAD_DIR, processed_zip_filename)

            shutil.make_archive(processed_zip_path[:-4], 'zip', processed_data_dir)

            download_link = f"/api/download/{processed_zip_filename}"

            return {"message": "Data processed successfully!", "logs": logs, "download_link": download_link}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def process_data(data: MMLabsData, process_uuid: str, output_dir: str):
    logs = []
    process_dir = os.path.join(output_dir, process_uuid)
    os.makedirs(process_dir, exist_ok=True)

    logger.debug(f"Processing data for process UUID: {process_uuid}")
    logger.debug(f"Output directory: {process_dir}")

    measurements = data.measurements.get(process_uuid, [])
    logger.debug(f"Number of measurements: {len(measurements)}")

    for measurement in measurements:
        measurement_uuid = measurement['uuid']
        set_id = measurement['set_id']
        measurement_start = datetime.strptime(measurement['measurement_start'], "%Y-%m-%dT%H:%M:%S.%f%z")
        measurement_date = measurement_start.strftime("%Y-%m-%d")
        
        measurement_dir = os.path.join(process_dir, measurement_date, set_id)
        os.makedirs(measurement_dir, exist_ok=True)

        logger.debug(f"Processing measurement: {measurement_uuid}")
        logger.debug(f"Measurement directory: {measurement_dir}")

        try:
            measurement_logs = process_measurement(data, process_uuid, measurement_uuid, measurement_dir)
            logs.extend(measurement_logs)
            logger.debug(f"Measurement {measurement_uuid} processed successfully")
        except Exception as e:
            error_message = f"Error processing measurement {measurement_uuid}: {str(e)}"
            logs.append(error_message)
            logger.error(error_message)

    logger.debug(f"Finished processing all measurements")
    return logs

def process_data(data: MMLabsData, process_uuid: str, output_dir: str):
    logs = []
    process_base_dir = os.path.join(output_dir, process_uuid)
    
    # Save process metadata and layout once at process level
    metadata_path = os.path.join(process_base_dir, 'process_metadata.json')
    measurements_path = os.path.join(process_base_dir, 'measurements.json')
    layout_path = os.path.join(process_base_dir, 'layout.png')
    os.makedirs(process_base_dir, exist_ok=True)
    
    if not os.path.exists(metadata_path):
        with open(metadata_path, 'w') as f:
            json.dump(data.processes[process_uuid], f, indent=2)
            logger.debug(f"Saved process metadata: {metadata_path}")
            logs.append(f"Saved process metadata: {metadata_path}")
            
    if not os.path.exists(measurements_path):
        with open(measurements_path, 'w') as f:
            json.dump(data.measurements[process_uuid], f, indent=2)
            logger.debug(f"Saved measurements data: {measurements_path}")
            logs.append(f"Saved measurements data: {measurements_path}")
            
    if not os.path.exists(layout_path):
        layout_bytes = data.get_layout_image(process_uuid)
        with open(layout_path, 'wb') as f:
            f.write(layout_bytes)
            logger.debug(f"Saved layout image: {layout_path}")
            logs.append(f"Saved layout image: {layout_path}")

    for measurement in data.measurements.get(process_uuid, []):
        measurement_uuid = measurement['uuid']
        logs.extend(process_measurement(data, process_uuid, measurement_uuid, process_base_dir))

    return logs

def process_data(data: MMLabsData, process_uuid: str, output_dir: str):
    logs = []
    process_base_dir = os.path.join(output_dir, process_uuid)
    
    # Save process metadata and layout once at process level
    metadata_path = os.path.join(process_base_dir, 'process_metadata.json')
    layout_path = os.path.join(process_base_dir, 'layout.png')
    os.makedirs(process_base_dir, exist_ok=True)
    
    if not os.path.exists(metadata_path):
        with open(metadata_path, 'w') as f:
            json.dump(data.processes[process_uuid], f, indent=2)
            logger.debug(f"Saved process metadata: {metadata_path}")
            logs.append(f"Saved process metadata: {metadata_path}")
            
    if not os.path.exists(layout_path):
        layout_bytes = data.get_layout_image(process_uuid)
        with open(layout_path, 'wb') as f:
            f.write(layout_bytes)
            logger.debug(f"Saved layout image: {layout_path}")
            logs.append(f"Saved layout image: {layout_path}")

    for measurement in data.measurements.get(process_uuid, []):
        measurement_uuid = measurement['uuid']
        logs.extend(process_measurement(data, process_uuid, measurement_uuid, process_base_dir))

    return logs

def process_measurement(data: MMLabsData, process_uuid: str, measurement_uuid: str, process_base_dir: str):
    logs = []
    measurement_path = data.get_measurement_dir_path(process_uuid, measurement_uuid)
    logger.debug(f"Processing measurement: {measurement_uuid}")

    # Get measurement data
    base_act_ts = get_base_activitiy_ts_for_measurement(measurement_path, remove_pauses=False)
    region_ts = get_region_ts_for_measurement(measurement_path, remove_pauses=False)
    handling_heights_ts = get_handling_heights_ts_for_measurement(measurement_path, remove_pauses=False)
    pause_ts = get_pause_ts_for_measurement(measurement_path)

    region_uuid_to_name = {r['uuid']: r['name'] for r in data.processes[process_uuid]["layout"]["regions"]}
    activity_id_to_name_map, _ = get_base_activity_plot_info(measurement_path)
    handling_height_id_to_name_map, _ = get_handling_heights_plot_info(measurement_path)

    measurement_info = next((m for m in data.measurements[process_uuid] if m['uuid'] == measurement_uuid), None)
    if not measurement_info:
        logs.append(f"Measurement info not found for {measurement_uuid}")
        return logs

    # Handle timezone conversion properly
    utc_time = datetime.strptime(measurement_info['measurement_start'], "%Y-%m-%dT%H:%M:%S.%f%z")
    germany_tz = pytz.timezone('Europe/Berlin')
    local_time = utc_time.astimezone(germany_tz)
    measurement_date = local_time.strftime("%Y-%m-%d")

    # Setup directory structure
    date_dir = os.path.join(process_base_dir, measurement_date)
    set_id_dir = os.path.join(date_dir, measurement_info['set_id'])
    beacons_dir = os.path.join(date_dir, "beacons")
    os.makedirs(set_id_dir, exist_ok=True)
    os.makedirs(beacons_dir, exist_ok=True)

    # Calculate time in seconds with proper timezone offset
    local_midnight = local_time.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time_seconds = (local_time - local_midnight).total_seconds()
    
    df_activity = pd.DataFrame({
        'id': measurement_info['set_id'],
        'startTime': [round(start_time_seconds + i * 0.1, 3) for i in range(len(base_act_ts))],
        'endTime': [round(start_time_seconds + (i + 1) * 0.1, 3) for i in range(len(base_act_ts))],
        'region': [str(region_uuid_to_name.get(r, 'Unknown Region')) for r in region_ts],
        'activity': [str(handling_height_id_to_name_map[h]) if activity_id_to_name_map[a] == 'Handling' 
                    else str(activity_id_to_name_map[a]) for a, h in zip(base_act_ts, handling_heights_ts)],
        'isPauseData': pause_ts
    })

    activity_filename = f"{measurement_info['set_id']}_{local_time.strftime('%Y-%m-%dT%H_%M_%S')}.csv"
    activity_csv_path = os.path.join(set_id_dir, activity_filename)
    df_activity.to_csv(activity_csv_path, index=False)
    logger.debug(f"Saved activity data: {activity_csv_path}")
    logs.append(f"Saved activity data: {activity_csv_path}")

    # Process beacon data
    try:
        closeness_arr, usage_arr, beacon_uuids = get_dynamic_beacon_data_for_measurement(measurement_path, remove_pauses=False)
        beacon_metadata = {d['uuid']: d for d in data.processes[process_uuid]["layout"]["dynamic_beacons"]}
        
        for idx, beacon_uuid in enumerate(beacon_uuids):
            beacon_uuid_str = str(beacon_uuid)
            beacon_metadata_entry = beacon_metadata.get(beacon_uuid_str, beacon_metadata.get(beacon_uuid, {}))
            beacon_name = str(beacon_metadata_entry.get('comment', beacon_uuid_str))
            beacon_dir = os.path.join(beacons_dir, beacon_name)
            os.makedirs(beacon_dir, exist_ok=True)
            
            df_beacon = pd.DataFrame({
                'id': measurement_info['set_id'],
                'startTime': [round(start_time_seconds + i * 0.1, 3) for i in range(len(closeness_arr))],
                'endTime': [round(start_time_seconds + (i + 1) * 0.1, 3) for i in range(len(closeness_arr))],
                'region': [str(region_uuid_to_name.get(region_ts[i], 'Unknown Region')) for i in range(len(closeness_arr))],
                'isNearby': closeness_arr[:, idx].astype(bool),
                'isUsing': usage_arr[:, idx].astype(bool)
            })
            
            # Filter rows where either isNearby or isUsing is True
            df_beacon = df_beacon[
                (df_beacon['isNearby']) | (df_beacon['isUsing'])
            ].copy()
            
            # Only save if there are any relevant interactions
            if not df_beacon.empty:
                # Include beacon name in filename
                beacon_filename = f"{beacon_name}_{activity_filename}"
                beacon_csv_path = os.path.join(beacon_dir, beacon_filename)
                df_beacon.to_csv(beacon_csv_path, index=False)
                logger.debug(f"Saved beacon data ({beacon_name}): {beacon_csv_path}")
                logs.append(f"Saved beacon data ({beacon_name}): {beacon_csv_path}")
            else:
                logger.debug(f"No relevant beacon interactions for {beacon_name}")
                logs.append(f"No relevant beacon interactions for {beacon_name}")

    except Exception as e:
        logger.error(f"Error processing beacon data: {str(e)}")
        logs.append(f"Error processing beacon data: {str(e)}")

    return logs

@app.get("/api/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type='application/octet-stream', filename=filename)
    raise HTTPException(status_code=404, detail="File not found")

@app.get("/")
def hello_world():
    return {"message": "Server is running!"}