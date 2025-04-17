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

## 3. **Data Fetching in `generate_report`**

- Fetches the last 7 days of `store_status` data (store_id, status, timestamp_utc).
- Fetches all `menu_hours` (store open/close times).
- Fetches all `timezones` (store_id, timezone_str).

**Purpose:**
To minimize memory usage and only process relevant data for the report.

---

## 4. **Timezone Handling**

- Merges `store_status` with `timezones` on `store_id`.
- Fills missing timezones with a default (`America/Chicago`).
- All timestamps are kept in UTC for calculations.
- When determining open/close intervals, local times from `menu_hours` are converted to UTC using the store's timezone.

**Purpose:**
To ensure all time calculations are accurate and comparable, regardless of the store's local timezone.

---

## 5. **Padding Logic**

The padding logic ensures that the store's status is defined for the entire reporting window, even if there are no status records exactly at the start or end of the window.

**Example:**

Suppose you want to calculate uptime for a store between 10:00 and 12:00, but the status records are:

- 10:15: "active"
- 11:30: "inactive"

There is no record at 10:00 or 12:00.

**Padding at Start:**
Since the first record is at 10:15 (after 10:00), the code checks for the last known status before 10:00. If there is one, it uses that; if not, it uses the first available status (here, "active"). It inserts a row at 10:00 with this status.

**Padding at End:**
The last record is at 11:30 (before 12:00), so the code inserts a row at 12:00 with the last known status ("inactive").

**Resulting intervals:**

- 10:00–10:15: "active" (from padding)
- 10:15–11:30: "active"
- 11:30–12:00: "inactive" (from padding)

This way, the entire window (10:00–12:00) is covered, and uptime/downtime can be calculated accurately.

---

## 6. **Open Hours Calculation**

- For each store and each day, retrieves open intervals from `menu_hours`.
- Converts these intervals from local time to UTC using the store's timezone.
- Only considers uptime/downtime during these open intervals.

**Purpose:**
To ensure that only the time when the store is scheduled to be open is counted towards uptime/downtime.

---

## 7. **Uptime/Downtime Calculation**

- For each store, for each reporting window (last hour, day, week):
  - Filters status records to the window.
  - Pads at start/end if needed.
  - For each interval between status records:
    - For each day in the interval, finds open intervals.
    - Calculates the overlap (in minutes) between the status interval and each open interval.
    - Sums up uptime (if status is 'active') and downtime (if 'inactive').

**Purpose:**
To accurately compute the total minutes/hours the store was up or down, but only during scheduled open hours.
