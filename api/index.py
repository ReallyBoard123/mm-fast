from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
import tempfile
import shutil
import uuid
from .main import setup_api, list_processes, download_data_for_process, extract_data

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://nextjs-frontend-b6w0.onrender.com"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TokenRequest(BaseModel):
    token: str
    save_folder: Optional[str] = "default_folder"

TEMP_DOWNLOAD_DIR = "/tmp/downloads"
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

@app.post("/api/process")
async def process_data(request: TokenRequest):
    try:
        print(f"Received token: {request.token}")  # Log the received token
        with tempfile.TemporaryDirectory() as temp_dir:
            save_folder = os.path.join(temp_dir, "processed_data")
            os.makedirs(save_folder, exist_ok=True)

            data = setup_api(request.token, save_folder)
            if not data:
                print("Error setting up API with the provided token.")
                raise HTTPException(status_code=500, detail="Error setting up API with the provided token.")

            processes_df = list_processes(data)
            if processes_df.empty:
                print("No processes found.")
                raise HTTPException(status_code=404, detail="No processes found.")

            logs = []
            for process_uuid in processes_df["process_uuid"]:
                download_data_for_process(data, process_uuid)
                process_logs = extract_data(data, process_uuid, save_folder)
                logs.extend(process_logs)

            zip_filename = f"{uuid.uuid4()}.zip"
            zip_path = os.path.join(TEMP_DOWNLOAD_DIR, zip_filename)
            shutil.make_archive(zip_path[:-4], 'zip', save_folder)

            download_link = f"/api/download/{zip_filename}"
            print(f"Processed successfully, download link: {download_link}")  # Log success
            return {"message": "Data processed successfully!", "logs": logs, "download_link": download_link}
    except Exception as e:
        print(f"Error in /api/process: {e}")  # Log the error
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type='application/octet-stream', filename=filename)
    raise HTTPException(status_code=404, detail="File not found")

@app.get("/api/python")
def hello_world():
    return {"message": "Server is running!"}

@app.middleware("http")
async def log_requests(request: Request, call_next):
    print(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    print(f"Response: {response.status_code}")
    return response

@app.get("/")
def read_root():
    return {"message": "Hello World"}