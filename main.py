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
    
    # Configure batch processing
    BATCH_SIZE = 50000  # Adjust based on memory constraints
    
    with engine.connect() as conn:
        # Get the max timestamp to define the current time and time windows
        max_time_query = pd.read_sql("SELECT MAX(timestamp_utc) as max_time FROM store_status", conn)
        current_time = pd.to_datetime(max_time_query['max_time'][0])
        
        # Define time windows once
        last_hour = current_time - timedelta(hours=1)
        last_day = current_time - timedelta(days=1)
        last_week = current_time - timedelta(days=7)
        
        # Get unique store IDs to process in batches
        store_ids = pd.read_sql("SELECT DISTINCT store_id FROM store_status", conn)
        
        # Get timezone data for all stores (small table, fetch once)
        timezones = pd.read_sql("SELECT * FROM timezones", conn)
        
        report_rows = []
        
        # Process stores in batches
        total_stores = len(store_ids)
        for batch_start in range(0, total_stores, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_stores)
            batch_ids = store_ids.iloc[batch_start:batch_end]['store_id'].tolist()
            
            # Create placeholders for the IN clause
            placeholders = ','.join([f"'{store_id}'" for store_id in batch_ids])
            
            # Fetch only needed data for the batch of stores
            store_status_batch = pd.read_sql(
                f"""SELECT store_id, status, timestamp_utc 
                   FROM store_status 
                   WHERE store_id IN ({placeholders})
                   AND timestamp_utc >= '{last_week}'""", conn
            )
            
            if store_status_batch.empty:
                continue
                
            # Convert timestamp to datetime once
            store_status_batch['timestamp_utc'] = pd.to_datetime(store_status_batch['timestamp_utc'])
            store_status_batch['status'] = store_status_batch['status'].str.lower()
            
            # Merge timezone data for this batch
            store_status_batch = store_status_batch.merge(timezones, on='store_id', how='left')
            store_status_batch['timezone_str'] = store_status_batch['timezone_str'].fillna('America/Chicago')
            
            # Process each store in the current batch
            for store_id, group in store_status_batch.groupby('store_id'):
                # Sort once for all time windows
                group_sorted = group.sort_values('timestamp_utc')
                
                # Compute metrics for all time windows in one pass
                metrics = compute_metrics_optimized(group_sorted, current_time, [last_hour, last_day, last_week])
                
                report_rows.append({
                    "store_id": store_id,
                    "uptime_last_hour": metrics[0][0],
                    "uptime_last_day": round(metrics[1][0] / 60, 2),
                    "uptime_last_week": round(metrics[2][0] / 60, 2),
                    "downtime_last_hour": metrics[0][1],
                    "downtime_last_day": round(metrics[1][1] / 60, 2),
                    "downtime_last_week": round(metrics[2][1] / 60, 2)
                })
            
            print(f"Processed batch {batch_start} to {batch_end} of {total_stores} stores")
    
    df_out = pd.DataFrame(report_rows)
    file_path = f"report_{report_id}.csv"
    df_out.to_csv(file_path, index=False)
    reports[report_id] = file_path
    end_time = time.time()
    print(f"[{datetime.datetime.now()}] Report generation finished for report_id={report_id}")
    print(f"Time taken for report_id={report_id}: {end_time - start_time:.2f} seconds")

def compute_metrics_optimized(group, current_time, time_windows):
    """
    Compute metrics for multiple time windows in one pass
    
    Args:
        group: DataFrame containing store status data for a single store
        current_time: The reference "current time"
        time_windows: List of start times for each window [last_hour, last_day, last_week]
        
    Returns:
        List of (uptime, downtime) tuples for each time window
    """
    results = []
    
    for start_time in time_windows:
        # Filter data for current time window
        df = group[(group['timestamp_utc'] >= start_time) & (group['timestamp_utc'] <= current_time)].copy()
        
        # If no polls in interval, extrapolate from closest poll before start_time
        if df.empty:
            prev = group[group['timestamp_utc'] < start_time]
            if not prev.empty:
                last_known = prev.iloc[-1]
                status = last_known['status']
                duration = (current_time - start_time).total_seconds() / 60
                results.append((duration if status == 'active' else 0, duration if status != 'active' else 0))
            else:
                results.append((0, 0))
            continue
            
        # Prepare data for padding at start and end if needed
        need_start_pad = df.iloc[0]['timestamp_utc'] > start_time
        need_end_pad = df.iloc[-1]['timestamp_utc'] < current_time
        
        # Create a new dataframe with padding records for vectorized operations
        timestamps = list(df['timestamp_utc'])
        statuses = list(df['status'])
        
        # Add padding at start if needed
        if need_start_pad:
            prev = group[group['timestamp_utc'] < start_time]
            initial_status = prev.iloc[-1]['status'] if not prev.empty else df.iloc[0]['status']
            timestamps.insert(0, start_time)
            statuses.insert(0, initial_status)
            
        # Add padding at end if needed
        if need_end_pad:
            final_status = df.iloc[-1]['status']
            timestamps.append(current_time)
            statuses.append(final_status)
            
        # Create DataFrame with padded values
        padded_df = pd.DataFrame({
            'timestamp_utc': timestamps,
            'status': statuses
        }).sort_values('timestamp_utc').reset_index(drop=True)
        
        # Calculate duration using vectorized operations
        padded_df['next_timestamp'] = padded_df['timestamp_utc'].shift(-1)
        padded_df['duration'] = (padded_df['next_timestamp'] - padded_df['timestamp_utc']).dt.total_seconds() / 60
        padded_df = padded_df.dropna()  # Drop the last row with NaN duration
        
        # Use vectorized operations to calculate uptime and downtime
        uptime = padded_df.loc[padded_df['status'] == 'active', 'duration'].sum()
        downtime = padded_df.loc[padded_df['status'] == 'inactive', 'duration'].sum()
        
        results.append((uptime, downtime))
        
    return results
