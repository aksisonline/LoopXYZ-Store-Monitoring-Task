# Store Monitoring Task - Code Documentation

This document explains the implementation of the main report generation logic, block by block, in `main.py`.

---

## 1. **Database Connection and Setup**

- Loads environment variables for database credentials.
- Constructs a SQLAlchemy connection string.
- Connects to the PostgreSQL database using SQLAlchemy.

**Purpose:**
To securely and flexibly connect to the database for fetching required data.

---

## 2. **API Endpoints**

- `/trigger_report`: Starts report generation in the background and returns a unique `report_id`.
- `/get_report`: Checks the status of the report or returns the generated CSV file.

**Purpose:**
To provide asynchronous report generation and retrieval via a REST API.

---

## 3. **Data Processing in `generate_report`**

- Uses batch processing to manage memory usage (configurable batch size).
- Gets the max timestamp to define the current time.
- Defines time windows for analysis (last hour, day, and week).
- Fetches unique store IDs and processes them in batches.
- Fetches timezone data for all stores once (small table).
- Processes each batch of stores efficiently.

**Purpose:**
To optimize memory usage and processing time for large datasets.

---

## 4. **Batch Processing**

- Processes stores in configurable batches (default 50,000).
- For each batch:
  - Fetches only the relevant store status data.
  - Only retrieves data from the last week.
  - Processes each store in the batch individually.

**Purpose:**
To handle large datasets efficiently by limiting memory usage and enabling parallel processing.

---

## 5. **Timezone Handling**

- Merges `store_status` with `timezones` on `store_id`.
- Fills missing timezones with a default (`America/Chicago`).
- Keeps timestamps in UTC for consistent calculations.

**Purpose:**
To ensure all time calculations are accurate and comparable, regardless of the store's local timezone.

---

## 6. **Performance Optimization**

- Converts timestamps to datetime once, reducing redundant operations.
- Normalizes status strings to lowercase for consistent comparison.
- Sorts status records once for all time windows.
- Computes metrics for all time windows in a single pass.
- Uses vectorized operations for calculations when possible.

**Purpose:**
To maximize processing efficiency and minimize execution time.

---

## 7. **Padding Logic**

The padding logic ensures that the store's status is defined for the entire reporting window, even if there are no status records exactly at the start or end of the window.

**Implementation:**

- For each time window, checks if data exists at start and end points.
- Adds padding at start using previous status (if available) or first available status.
- Adds padding at end using the last known status.
- Creates a new dataframe with padded values for vectorized operations.

**Purpose:**
To ensure accurate calculation of uptime/downtime across the entire time window.

---

## 8. **Uptime/Downtime Calculation**

The `compute_metrics_optimized` function calculates uptime and downtime for multiple time windows in a single pass:

- For each time window (last hour, day, week):
  - Filters data to the current window.
  - If no data is available, extrapolates from the last known status.
  - Adds padding at start/end if needed.
  - Uses vectorized operations to calculate durations between status changes.
  - Sums all durations where status is 'active' (uptime) or 'inactive' (downtime).

**Purpose:**
To efficiently and accurately compute the total minutes/hours a store was up or down during each reporting period.

---

## 9. **Output Format**

The generated report includes:

- Store ID
- Uptime in the last hour (minutes)
- Uptime in the last day (hours, rounded to 2 decimal places)
- Uptime in the last week (hours, rounded to 2 decimal places)
- Downtime in the last hour (minutes)
- Downtime in the last day (hours, rounded to 2 decimal places)
- Downtime in the last week (hours, rounded to 2 decimal places)

**Purpose:**
To provide a comprehensive overview of store operational status across different time periods.
