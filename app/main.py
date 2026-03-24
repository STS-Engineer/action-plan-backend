import os
import uvicorn

from fastapi import FastAPI
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from app.routers import ALL_ROUTERS

load_dotenv()

app = FastAPI(
    title="Action Plan API",
    description="API for managing action plans",
)

origins = [
    "http://localhost:5173",
    "http://localhost:3000",
    os.getenv("FRONTEND_URL"),
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for router in ALL_ROUTERS:
    app.include_router(router)
    
if __name__ == "__main__":
    uvicorn.run("main:app", host="localhost", port= 8000, reload=True)
