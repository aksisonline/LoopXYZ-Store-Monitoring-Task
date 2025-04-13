import os
import uuid
import csv
from datetime import datetime, timedelta
from typing import List, Dict
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, Session, declarative_base
import pandas as pd
import pytz
from pathlib import Path
import gradio as gr
from contextlib import asynccontextmanager

# Lifespan context manager for startup/shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: run data ingestion
    ingest_data()
    yield
    # Shutdown: cleanup code can go here if needed

# Initialize FastAPI app with lifespan
app = FastAPI(title="Loop XYZ - Store Monitoring API", lifespan=lifespan)

# Database setup (PostgreSQL)
DATABASE_URL = "postgresql://loopxyz_owner:npg_1E4lLCbiShYm@ep-young-bonus-a1cepd1n-pooler.ap-southeast-1.aws.neon.tech/loopxyz?sslmode=require"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database models (unchanged)
class StoreStatus(Base):
    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timestamp_utc = Column(DateTime)
    status = Column(String)

class MenuHours(Base):
    __tablename__ = "business_hours"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    day_of_week = Column(Integer)
    start_time_local = Column(String)
    end_time_local = Column(String)

class Timezone(Base):
    __tablename__ = "timezone"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timezone_str = Column(String)

class Report(Base):
    __tablename__ = "reports"
    report_id = Column(String, primary_key=True, index=True)
    status = Column(String, default="Running")
    created_at = Column(DateTime, default=datetime.utcnow)
    file_path = Column(String, nullable=True)

# Create database tables
Base.metadata.create_all(bind=engine)

# Data ingestion (unchanged)
def ingest_data():
    db: Session = SessionLocal()
    try:
        status_df = pd.read_csv("/kaggle/input/loopxyz/store_status.csv")
        for _, row in status_df.iterrows():
            status = StoreStatus(
                store_id=str(row["store_id"]),
                timestamp_utc=pd.to_datetime(row["timestamp_utc"]),
                status=row["status"]
            )
            db.merge(status)

        if os.path.exists("kaggle/input/loopxyz/menu_hours.csv"):
            hours_df = pd.read_csv("/kaggle/input/loopxyz/menu_hours.csv")
            for _, row in hours_df.iterrows():
                hours = MenuHours(
                    store_id=str(row["store_id"]),
                    day_of_week=int(row["day_of_week"]),
                    start_time_local=row["start_time_local"],
                    end_time_local=row["end_time_local"]
                )
                db.merge(hours)

        if os.path.exists("/kaggle/input/loopxyz/timezones.csv"):
            tz_df = pd.read_csv("/kaggle/input/loopxyz/timezones.csv")
            for _, row in tz_df.iterrows():
                tz = Timezone(
                    store_id=str(row["store_id"]),
                    timezone_str=row["timezone_str"]
                )
                db.merge(tz)

        db.commit()
    finally:
        db.close()

# Helper functions (unchanged)
def get_timezone(db: Session, store_id: str) -> str:
    tz = db.query(Timezone).filter(Timezone.store_id == store_id).first()
    return tz.timezone_str if tz else "America/Chicago"

def get_business_hours(db: Session, store_id: str) -> List[Dict]:
    hours = db.query(MenuHours).filter(MenuHours.store_id == store_id).all()
    if not hours:
        return [{"day_of_week": d, "start_time_local": "00:00", "end_time_local": "23:59"} for d in range(7)]
    return [
        {
            "day_of_week": h.day_of_week,
            "start_time_local": h.start_time_local,
            "end_time_local": h.end_time_local
        }
        for h in hours
    ]

def calculate_uptime_downtime(
    store_id: str,
    status_data: pd.DataFrame,
    business_hours: List[Dict],
    timezone_str: str,
    current_time: datetime
) -> Dict:
    tz = pytz.timezone(timezone_str)
    uptime_last_hour = 0.0
    downtime_last_hour = 0.0
    uptime_last_day = 0.0
    downtime_last_day = 0.0
    uptime_last_week = 0.0
    downtime_last_week = 0.0

    status_data["timestamp_local"] = status_data["timestamp_utc"].dt.tz_convert(tz)

    for period, delta, unit in [
        ("last_hour", timedelta(hours=1), "minutes"),
        ("last_day", timedelta(days=1), "hours"),
        ("last_week", timedelta(days=7), "hours")
    ]:
        start_time = current_time - delta
        period_data = status_data[
            (status_data["timestamp_utc"] >= start_time) &
            (status_data["timestamp_utc"] <= current_time)
        ].sort_values("timestamp_local")

        total_uptime = 0.0
        total_downtime = 0.0
        total_business_minutes = 0.0

        current_day = start_time
        while current_day <= current_time:
            day_end = min(current_day + timedelta(days=1), current_time)
            local_day = current_day.astimezone(tz)
            day_of_week = local_day.weekday()
            day_of_week = (day_of_week + 1) % 7

            day_hours = [h for h in business_hours if h["day_of_week"] == day_of_week]
            if not day_hours:
                current_day += timedelta(days=1)
                continue

            for bh in day_hours:
                start_time_str = bh["start_time_local"]
                end_time_str = bh["end_time_local"]
                try:
                    start_hour, start_min = map(int, start_time_str.split(":"))
                    end_hour, end_min = map(int, end_time_str.split(":"))
                except ValueError:
                    continue

                bh_start = local_day.replace(hour=start_hour, minute=start_min, second=0, microsecond=0)
                bh_end = local_day.replace(hour=end_hour, minute=end_min, second=0, microsecond=0)
                if bh_end <= bh_start:
                    bh_end += timedelta(days=1)

                if bh_end < start_time.astimezone(tz) or bh_start > current_time.astimezone(tz):
                    continue

                bh_start = max(bh_start, start_time.astimezone(tz))
                bh_end = min(bh_end, current_time.astimezone(tz))
                business_duration = (bh_end - bh_start).total_seconds() / 60
                total_business_minutes += business_duration

                window_data = period_data[
                    (period_data["timestamp_local"] >= bh_start) &
                    (period_data["timestamp_local"] <= bh_end)
                ]

                if window_data.empty:
                    total_downtime += business_duration
                    continue

                prev_time = bh_start
                prev_status = "inactive"
                for _, row in window_data.iterrows():
                    curr_time = row["timestamp_local"]
                    curr_status = row["status"]
                    duration = (curr_time - prev_time).total_seconds() / 60
                    if duration > 0:
                        if prev_status == "active":
                            total_uptime += duration
                        else:
                            total_downtime += duration
                    prev_time = curr_time
                    prev_status = curr_status

                duration = (bh_end - prev_time).total_seconds() / 60
                if duration > 0:
                    if prev_status == "active":
                        total_uptime += duration
                    else:
                        total_downtime += duration

            current_day += timedelta(days=1)

        if unit == "minutes":
            factor = 1.0
        else:
            factor = 60.0
        if total_business_minutes > 0:
            if period == "last_hour":
                uptime_last_hour = total_uptime
                downtime_last_hour = total_downtime
            elif period == "last_day":
                uptime_last_day = total_uptime / factor
                downtime_last_day = total_downtime / factor
            else:
                uptime_last_week = total_uptime / factor
                downtime_last_week = total_downtime / factor

    return {
        "store_id": store_id,
        "uptime_last_hour": round(uptime_last_hour, 2),
        "downtime_last_hour": round(downtime_last_hour, 2),
        "uptime_last_day": round(uptime_last_day, 2),
        "downtime_last_day": round(downtime_last_day, 2),
        "uptime_last_week": round(uptime_last_week, 2),
        "downtime_last_week": round(downtime_last_week, 2)
    }

def generate_report(report_id: str):
    db: Session = SessionLocal()
    try:
        max_timestamp = db.query(StoreStatus.timestamp_utc).order_by(StoreStatus.timestamp_utc.desc()).first()
        current_time = max_timestamp[0] if max_timestamp else datetime.utcnow().replace(tzinfo=pytz.UTC)

        store_ids = [s[0] for s in db.query(StoreStatus.store_id).distinct().all()]
        report_data = []

        for store_id in store_ids:
            status_data = pd.read_sql(
                db.query(StoreStatus).filter(StoreStatus.store_id == store_id).statement,
                db.connection()
            )
            status_data["timestamp_utc"] = pd.to_datetime(status_data["timestamp_utc"])
            business_hours = get_business_hours(db, store_id)
            timezone_str = get_timezone(db, store_id)

            metrics = calculate_uptime_downtime(
                store_id, status_data, business_hours, timezone_str, current_time
            )
            report_data.append(metrics)

        os.makedirs("reports", exist_ok=True)
        file_path = f"reports/{report_id}.csv"
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "store_id", "uptime_last_hour", "downtime_last_hour",
                    "uptime_last_day", "downtime_last_day",
                    "uptime_last_week", "downtime_last_week"
                ]
            )
            writer.writeheader()
            writer.writerows(report_data)

        report = db.query(Report).filter(Report.report_id == report_id).first()
        report.status = "Complete"
        report.file_path = file_path
        db.commit()
    except Exception as e:
        report = db.query(Report).filter(Report.report_id == report_id).first()
        report.status = "Failed"
        db.commit()
        raise e
    finally:
        db.close()

# API Endpoints
@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    db: Session = SessionLocal()
    try:
        report = Report(report_id=report_id, status="Running")
        db.add(report)
        db.commit()
        background_tasks.add_task(generate_report, report_id)
        return {"report_id": report_id}
    finally:
        db.close()

@app.get("/get_report")
async def get_report(report_id: str):
    db: Session = SessionLocal()
    try:
        report = db.query(Report).filter(Report.report_id == report_id).first()
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        if report.status == "Running":
            return {"status": "Running"}
        elif report.status == "Complete" and report.file_path:
            return FileResponse(
                report.file_path,
                media_type="text/csv",
                filename=f"report_{report_id}.csv",
                headers={"status": "Complete"}
            )
        else:
            raise HTTPException(status_code=500, detail="Report generation failed")
    finally:
        db.close()

# Gradio Interface
def trigger_report_ui():
    """Trigger a new report generation"""
    db: Session = SessionLocal()
    try:
        report_id = str(uuid.uuid4())
        report = Report(report_id=report_id, status="Running")
        db.add(report)
        db.commit()
        
        # Start report generation (non-background for demo)
        try:
            generate_report(report_id)
            return f"Report triggered successfully! Your report ID is: {report_id}"
        except Exception as e:
            return f"Error generating report: {str(e)}"
    finally:
        db.close()

def check_report_status_ui(report_id: str):
    """Check status of a report by ID"""
    if not report_id:
        return "Please enter a report ID"
        
    db: Session = SessionLocal()
    try:
        report = db.query(Report).filter(Report.report_id == report_id).first()
        if not report:
            return "Report not found"
            
        if report.status == "Running":
            return "Report is still being generated. Please check back later."
        elif report.status == "Complete" and report.file_path:
            # Return download link
            file_path = Path(report.file_path)
            if file_path.exists():
                return f"Report is complete. Download link: {file_path}"
            else:
                return "Report file not found."
        else:
            return "Report generation failed"
    finally:
        db.close()

# Create Gradio interface
with gr.Blocks(title="Loop XYZ Store Monitoring API") as demo:
    gr.Markdown("# Loop XYZ Store Monitoring")
    
    with gr.Tab("Trigger Report"):
        trigger_button = gr.Button("Generate New Report")
        trigger_output = gr.Textbox(label="Result")
        trigger_button.click(fn=trigger_report_ui, outputs=trigger_output)
    
    with gr.Tab("Check Report Status"):
        report_id_input = gr.Textbox(label="Report ID")
        check_button = gr.Button("Check Status")
        status_output = gr.Textbox(label="Status")
        check_button.click(fn=check_report_status_ui, inputs=report_id_input, outputs=status_output)

# Create Gradio public link
if __name__ == "__main__":
    demo.launch(share=True)  # share=True creates a public link