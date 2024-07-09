from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://mm-fast.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/python")
def hello_world():
    return {"message": "Hello World"}

# For Vercel serverless function
from fastapi.responses import JSONResponse

def handler(request):
    # Delegate request to FastAPI app
    response = app(request)

    # Convert FastAPI response to JSON response
    content = response.body.decode()
    return JSONResponse(content=content)