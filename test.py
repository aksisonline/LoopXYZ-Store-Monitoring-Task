#!/usr/bin/env python
import requests
import json
import time
import os
import sys

# Configure the base URL for API calls
BASE_URL = "http://localhost:8000"

def clear_screen():
    """Clear the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def trigger_report():
    """Call the trigger_report API and return the report_id."""
    try:
        response = requests.post(f"{BASE_URL}/trigger_report")
        if response.status_code == 200:
            result = response.json()
            print(f"Report generation triggered successfully!")
            print(f"Report ID: {result['report_id']}")
            return result['report_id']
        else:
            print(f"Error: API returned status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the API: {e}")
        return None

def get_report(report_id):
    """Get the report status or download the report if completed."""
    try:
        response = requests.get(f"{BASE_URL}/get_report?report_id={report_id}")
        
        if response.status_code == 200:
            # Check if the response is JSON (status) or a file (completed report)
            content_type = response.headers.get('Content-Type', '')
            
            if 'application/json' in content_type:
                # It's a status response
                result = response.json()
                print(f"Report status: {result['status']}")
                return False
            elif 'text/csv' in content_type:
                # It's a completed report
                filename = f"downloaded_report_{report_id}.csv"
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f"Report downloaded successfully as '{filename}'")
                return True
            else:
                print(f"Unexpected content type: {content_type}")
                return False
        else:
            print(f"Error: API returned status code {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the API: {e}")
        return False

def poll_report(report_id, interval=5, max_attempts=20):
    """Poll the report status until it's complete or max_attempts is reached."""
    print(f"Polling report status every {interval} seconds...")
    
    for attempt in range(max_attempts):
        print(f"Attempt {attempt + 1}/{max_attempts}...")
        
        if get_report(report_id):
            # Report is complete and downloaded
            return True
        
        print(f"Waiting {interval} seconds for next attempt...")
        time.sleep(interval)
    
    print("Maximum polling attempts reached. Report might still be processing.")
    return False

def main():
    """Main function to provide a terminal UI for API testing."""
    report_id = None
    
    while True:
        clear_screen()
        print("==== Store Monitoring API Test Tool ====")
        print("1. Trigger a new report")
        print("2. Check report status")
        print("3. Check and poll report status")
        print("4. Exit")
        
        if report_id:
            print(f"\nCurrent report ID: {report_id}")
        
        choice = input("\nEnter your choice (1-4): ")
        
        if choice == '1':
            new_report_id = trigger_report()
            if new_report_id:
                report_id = new_report_id
            input("\nPress Enter to continue...")
        
        elif choice == '2':
            if not report_id:
                report_id = input("Enter report ID to check: ")
            get_report(report_id)
            input("\nPress Enter to continue...")
        
        elif choice == '3':
            if not report_id:
                report_id = input("Enter report ID to poll: ")
            
            interval = input("Enter polling interval in seconds (default 5): ")
            interval = int(interval) if interval.isdigit() else 5
            
            max_attempts = input("Enter maximum polling attempts (default 20): ")
            max_attempts = int(max_attempts) if max_attempts.isdigit() else 20
            
            poll_report(report_id, interval, max_attempts)
            input("\nPress Enter to continue...")
        
        elif choice == '4':
            print("Exiting...")
            sys.exit(0)
        
        else:
            print("Invalid choice. Please try again.")
            input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()