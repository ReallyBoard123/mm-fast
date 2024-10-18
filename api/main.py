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

def process_measurement(data: MMLabsData, process_uuid: str, measurement_uuid: str, output_dir: str):
    logs = []
    measurement_path = data.get_measurement_dir_path(process_uuid, measurement_uuid)
    
    logger.debug(f"Processing measurement: {measurement_uuid}")
    logger.debug(f"Measurement path: {measurement_path}")
    logger.debug(f"Output directory: {output_dir}")

    base_act_ts = get_base_activitiy_ts_for_measurement(measurement_path, remove_pauses=False)
    region_ts = get_region_ts_for_measurement(measurement_path, remove_pauses=False)
    handling_heights_ts = get_handling_heights_ts_for_measurement(measurement_path, remove_pauses=False)

    logger.debug(f"Base activity time series shape: {base_act_ts.shape}")
    logger.debug(f"Region time series shape: {region_ts.shape}")
    logger.debug(f"Handling heights time series shape: {handling_heights_ts.shape}")

    region_uuid_to_name = {r['uuid']: r['name'] for r in data.processes[process_uuid]["layout"]["regions"]}
    activity_id_to_name_map, _ = get_base_activity_plot_info(measurement_path)
    handling_height_id_to_name_map, _ = get_handling_heights_plot_info(measurement_path)

    measurement_info = next((m for m in data.measurements[process_uuid] if m['uuid'] == measurement_uuid), None)
    if not measurement_info:
        logs.append(f"Measurement info not found for {measurement_uuid}")
        return logs

    start_time = datetime.strptime(measurement_info['measurement_start'], "%Y-%m-%dT%H:%M:%S.%f%z")
    duration_sec = measurement_info['duration_sec']

    df = pd.DataFrame({
        'region': region_ts,
        'activity': base_act_ts,
        'handling_height': handling_heights_ts
    })

    start_time_seconds = start_time.hour * 3600 + start_time.minute * 60 + start_time.second + start_time.microsecond / 1e6
    start_time_seconds += 7200  # Add 7200 seconds (2 hours) for UTC+2
    df['Start Time'] = [round(start_time_seconds + i * 0.1, 3) for i in range(len(df))]
    df['End Time'] = df['Start Time'] + 0.1
    df['End Time'] = df['End Time'].round(3)

    logger.debug(f"DataFrame shape: {df.shape}")

    df['region_name'] = df['region'].map(lambda x: region_uuid_to_name.get(x, 'Unknown Region'))
    df['activity_name'] = df['activity'].map(activity_id_to_name_map)
    df['handling_height_name'] = df['handling_height'].map(handling_height_id_to_name_map)

    df['Activity'] = df.apply(lambda row: row['handling_height_name'] if row['activity_name'] == 'Handling' else row['activity_name'], axis=1)

    final_df = df[['Start Time', 'End Time', 'region_name', 'Activity']].copy()
    final_df.columns = ['Start Time', 'End Time', 'Region', 'Activity']
    final_df['Set ID'] = measurement_info['set_id']
    final_df = final_df[['Set ID', 'Start Time', 'End Time', 'Region', 'Activity']]

    # Add 2 hours to the start_time for the filename
    filename_time = start_time + timedelta(hours=2)
    csv_path = os.path.join(output_dir, f"{measurement_info['set_id']}_{filename_time.strftime('%Y-%m-%dT%H_%M_%S')}.csv")
    final_df.to_csv(csv_path, index=False)
    
    actual_duration = final_df['End Time'].iloc[-1] - final_df['Start Time'].iloc[0]
    duration_matches = abs(actual_duration - duration_sec) < 0.1  # Allow 0.1 second difference
    
    logs.append(f"Combined data saved to {csv_path}")
    logs.append(f"Expected duration: {duration_sec}, Actual duration: {actual_duration}")
    logs.append(f"Duration matches: {'Yes' if duration_matches else 'No'}")
    
    logger.debug(f"Combined data shape: {final_df.shape}")
    logger.debug(f"CSV saved: {os.path.exists(csv_path)}")
    logger.debug(f"Duration verification: {'Passed' if duration_matches else 'Failed'}")

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