from fastapi import FastAPI
from routers.api_router import api_router
from services import process_files
import asyncio

app=FastAPI()

@app.get("/")
def health_check():
    return {"message":"App running successfully"}


app.include_router(api_router)

# Start the background worker on startup
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_files())
