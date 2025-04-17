import uuid
import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
from datetime import timedelta
import os
from dotenv import load_dotenv
import time

app = FastAPI()

# Database connection
load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DB_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

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
    import datetime
    start_time = time.time()
    print(f"[{datetime.datetime.now()}] Report generation started for report_id={report_id}")
    with engine.connect() as conn:
        # Only fetch last 7 days of store_status, and only relevant columns
        max_time = pd.read_sql("SELECT MAX(timestamp_utc) as max_time FROM store_status", conn)['max_time'][0]
        max_time = pd.to_datetime(max_time)
        min_time = max_time - pd.Timedelta(days=7)
        store_status = pd.read_sql(
            f"SELECT store_id, status, timestamp_utc FROM store_status WHERE timestamp_utc >= '{min_time}'", conn
        )
        menu_hours = pd.read_sql("SELECT * FROM menu_hours", conn)
        timezones = pd.read_sql("SELECT * FROM timezones", conn)

    # Parse timestamp
    store_status['timestamp_utc'] = pd.to_datetime(store_status['timestamp_utc'])

    # Merge timezone info in advance for vectorized conversion
    store_status = store_status.merge(timezones, on='store_id', how='left')
    store_status['timezone_str'] = store_status['timezone_str'].fillna('America/Chicago')

    # Hardcoded "current time" as max timestamp
    current_time = store_status['timestamp_utc'].max()

    # Vectorized timezone conversion
    def convert_to_local(row):
        ts = row['timestamp_utc']
        tz = row['timezone_str']
        if ts.tzinfo is None:
            return ts.tz_localize('UTC').tz_convert(tz)
        else:
            return ts.tz_convert(tz)
    store_status['timestamp_local'] = store_status.apply(convert_to_local, axis=1)
    store_status['status'] = store_status['status'].str.lower()

    # Precompute time windows
    last_hour = current_time - timedelta(hours=1)
    last_day = current_time - timedelta(days=1)
    last_week = current_time - timedelta(days=7)

    report_rows = []

    # Group by store_id for vectorized calculation
    for store_id, group in store_status.groupby('store_id'):
        # No need to sort, as filtering is vectorized
        def compute_metrics(start_time):
            df = group[(group['timestamp_utc'] >= start_time) & (group['timestamp_utc'] <= current_time)]
            uptime = (df['status'] == 'active').sum() * 5
            downtime = (df['status'] == 'inactive').sum() * 5
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
    end_time = time.time()
    print(f"[{datetime.datetime.now()}] Report generation finished for report_id={report_id}")
    print(f"Time taken for report_id={report_id}: {end_time - start_time:.2f} seconds")
