import uuid
import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
from datetime import timedelta
import os
from dotenv import load_dotenv

app = FastAPI()

# Database connection
load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("DB_URL environment variable is not set in .env file")
engine = create_engine(DB_URL)

# In-memory report store
reports = {}

@app.post("/trigger_report")
def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    reports[report_id] = "Running"
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report")
def get_report(report_id: str):
    status = reports.get(report_id)
    if not status:
        raise HTTPException(status_code=404, detail="Report not found")
    if status == "Running":
        return {"status": "Running"}
    
    file_path = status
    if not os.path.exists(file_path):
        raise HTTPException(status_code=500, detail="Report file missing")
    
    return FileResponse(
        path=file_path,
        media_type='text/csv',
        filename=os.path.basename(file_path)
    )


def generate_report(report_id: str):
    with engine.connect() as conn:
        # Load all required tables
        store_status = pd.read_sql("SELECT * FROM store_status", conn)
        business_hours = pd.read_sql("SELECT * FROM business_hours", conn)
        timezones = pd.read_sql("SELECT * FROM timezone", conn)

    # Parse timestamp
    store_status['timestamp_utc'] = pd.to_datetime(store_status['timestamp_utc'])

    # Hardcoded "current time" as max timestamp
    current_time = store_status['timestamp_utc'].max()

    report_rows = []

    for store_id in store_status['store_id'].unique():
        store_data = store_status[store_status['store_id'] == store_id].sort_values("timestamp_utc")
        biz_hours = business_hours[business_hours['store_id'] == store_id]
        
        # Check if timezone data exists for this store
        timezone_data = timezones[timezones['store_id'] == store_id]
        if len(timezone_data) > 0:
            tz_str = timezone_data['timezone_str'].iloc[0]
        else:
            # Use America/Chicago as default timezone when missing
            tz_str = 'America/Chicago'
            print(f"No timezone found for store {store_id}, using America/Chicago as default.")

        store_data['timestamp_local'] = store_data['timestamp_utc'].dt.tz_localize('UTC').dt.tz_convert(tz_str)
        store_data['status'] = store_data['status'].str.lower()

        # Interpolate logic should go here — simplified version:
        # Assume observations are 5-minute polls. You can interpolate gaps later.
        last_hour = current_time - timedelta(hours=1)
        last_day = current_time - timedelta(days=1)
        last_week = current_time - timedelta(days=7)

        def compute_metrics(start_time):
            df = store_data[(store_data['timestamp_utc'] >= start_time) & (store_data['timestamp_utc'] <= current_time)]
            uptime = df[df['status'] == 'active'].shape[0] * 5
            downtime = df[df['status'] == 'inactive'].shape[0] * 5
            return uptime, downtime

        u1, d1 = compute_metrics(last_hour)
        u24, d24 = compute_metrics(last_day)
        u168, d168 = compute_metrics(last_week)

        report_rows.append({
            "store_id": store_id,
            "uptime_last_hour": u1,
            "uptime_last_day": round(u24 / 60, 2),
            "update_last_week": round(u168 / 60, 2),
            "downtime_last_hour": d1,
            "downtime_last_day": round(d24 / 60, 2),
            "downtime_last_week": round(d168 / 60, 2)
        })

    df_out = pd.DataFrame(report_rows)
    file_path = f"report_{report_id}.csv"
    df_out.to_csv(file_path, index=False)
    reports[report_id] = file_path
