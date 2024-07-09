from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mangum import Mangum
import os

app = FastAPI()

# Add CORS middleware only in development
if os.environ.get('VERCEL_ENV') != 'production':
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

@app.get("/api/python")
def hello_world():
    return JSONResponse({"message": "Hello World"})

# Mangum handler for AWS Lambda (used by Vercel)
handler = Mangum(app)