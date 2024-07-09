from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
from .main import setup_api, list_processes, download_data_for_process, extract_data

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://mm-fast.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TokenRequest(BaseModel):
    token: str
    save_folder: Optional[str] = "default_folder"

@app.post("/api/process")
async def process_data(request: TokenRequest):
    try:
        if not os.path.isabs(request.save_folder):
            raise HTTPException(status_code=400, detail="Save folder path must be absolute.")
        os.makedirs(request.save_folder, exist_ok=True)
        
        data = setup_api(request.token, request.save_folder)
        if not data:
            raise HTTPException(status_code=500, detail="Error setting up API with the provided token.")

        processes_df = list_processes(data)
        if processes_df.empty:
            raise HTTPException(status_code=404, detail="No processes found.")

        logs = []
        for process_uuid in processes_df["process_uuid"]:
            download_data_for_process(data, process_uuid)
            process_logs = extract_data(data, process_uuid, request.save_folder)
            logs.extend(process_logs)

        return {"message": "Data fetched and saved successfully!", "logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/python")
def hello_world():
    return {"message": "Server is running!"}
