import os
import sys
import requests
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

# --- ⚙️ Configuration ---
# Set these to the EXACT names of your Notion properties (case-sensitive)
TASK_PROP_DUE_DATE = "Due Date"
TASK_PROP_TITLE = "Tasks"
TASK_PROP_WEEKLY_LINK = "Weekly Link"
TASK_PROP_MONTHLY_LINK = "Monthly Link"
TASK_PROP_WEEK_NUMBER = "Week Number"  # Text: "41"
TASK_PROP_MONTH = "Month"  # Text: "October"
TASK_PROP_YEAR = "Year"  # Number: 2025

# Title property names in your "Weekly Progress" and "Monthly Progress" databases
WEEKLY_DB_TITLE_PROP = "Week Number"  # Text property: "1", "2", etc.
MONTHLY_DB_TITLE_PROP = "Month"  # Text property: "January", "February", etc.

# NEW: Year property names in Weekly/Monthly Progress databases
WEEKLY_DB_YEAR_PROP = "Year"  # Number property you'll add
MONTHLY_DB_YEAR_PROP = "Year"  # Number property you'll add

# --- Secrets & Initialization ---
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
TASKS_DB_ID = os.getenv("TASKS_DB_ID")
WEEKLY_DB_ID = os.getenv("WEEKLY_DB_ID")
MONTHLY_DB_ID = os.getenv("MONTHLY_DB_ID")

# Verify that all necessary secrets have been loaded
if not all([NOTION_API_KEY, TASKS_DB_ID, WEEKLY_DB_ID, MONTHLY_DB_ID]):
    print("❌ ERROR: Missing one or more required environment variables.")
    sys.exit(1)

# --- 📌 API Setup ---
HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_unlinked_tasks():
    """
    Queries for tasks that were recently edited, have a due date, 
    and are not yet linked to a weekly report.
    """
    # Calculate a timestamp for 65 minutes ago
    cut_off_time = (datetime.now(timezone.utc) - timedelta(minutes=65)).isoformat()

    url = f"https://api.notion.com/v1/databases/{TASKS_DB_ID}/query"

    payload = {
        "filter": {
            "and": [
                {
                    "property": TASK_PROP_DUE_DATE,
                    "date": {"is_not_empty": True}
                },
                {
                    "property": TASK_PROP_WEEKLY_LINK,
                    "relation": {"is_empty": True}
                },
                {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"on_or_after": cut_off_time}
                }
            ]
        }
    }

    try:
        print("🔎 Querying for unlinked tasks...")
        response = requests.post(url, headers=HEADERS, json=payload)
        
        if response.status_code != 200:
            print(f"❌ Error querying tasks: {response.text}")
            return []
            
        return response.json().get("results", [])
        
    except Exception as e:
        print(f"❌ Exception during query: {e}")
        return []


def extract_task_properties(task_properties):
    """
    Extracts year, week number (text), and month (text) from task properties.
    Returns: (year, week_text, month_text) tuple or (None, None, None) if error
    """
    try:
        # Extract year - should be a number formula
        year_prop = task_properties.get(TASK_PROP_YEAR, {}).get("formula", {})
        if year_prop.get("type") == "number":
            year = int(year_prop.get("number", 0))
        else:
            print(f"  ⚠️ Year property not found or not a number formula")
            return None, None, None

        # Extract week number - should be a text/string formula like "41"
        week_prop = task_properties.get(TASK_PROP_WEEK_NUMBER, {}).get("formula", {})
        week_text = week_prop.get("string")
        if not week_text:
            print(f"  ⚠️ Week Number property not found or empty")
            return None, None, None

        # Extract month - should be a text/string formula like "October"
        month_prop = task_properties.get(TASK_PROP_MONTH, {}).get("formula", {})
        month_text = month_prop.get("string")
        if not month_text:
            print(f"  ⚠️ Month property not found or empty")
            return None, None, None

        return year, week_text, month_text

    except Exception as e:
        print(f"  ⚠️ Error extracting task properties: {e}")
        return None, None, None


def find_weekly_page_with_year(week_text, year):
    """
    Searches for a Weekly Progress page matching BOTH week number (as text) AND year.
    
    Args:
        week_text: The week number as text (e.g., "1", "41")
        year: The year as number (e.g., 2025, 2026)
    
    Returns:
        Page ID if found, None otherwise
    """
    url = f"https://api.notion.com/v1/databases/{WEEKLY_DB_ID}/query"
    
    # Build filter to match BOTH title (text) AND year (number)
    payload = {
        "filter": {
            "and": [
                {
                    "property": WEEKLY_DB_TITLE_PROP,
                    "title": {"equals": week_text}
                },
                {
                    "property": WEEKLY_DB_YEAR_PROP,
                    "number": {"equals": year}
                }
            ]
        }
    }

    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        
        if response.status_code != 200:
            print(f"⚠️ Error searching weekly page: {response.text}")
            return None

        results = response.json().get("results", [])
        
        if results:
            page_id = results[0]["id"]
            print(f"  ✅ Found weekly page: Week {week_text} (Year: {year})")
            return page_id
        else:
            print(f"  ⚠️ No weekly page found with Week Number='{week_text}' and Year={year}")
            return None
            
    except Exception as e:
        print(f"❌ Exception finding weekly page: {e}")
        return None


def find_monthly_page_with_year(month_text, year):
    """
    Searches for a Monthly Progress page matching BOTH month name (as text) AND year.
    
    Args:
        month_text: The month name as text (e.g., "January", "October")
        year: The year as number (e.g., 2025, 2026)
    
    Returns:
        Page ID if found, None otherwise
    """
    url = f"https://api.notion.com/v1/databases/{MONTHLY_DB_ID}/query"
    
    # Build filter to match BOTH title (text) AND year (number)
    payload = {
        "filter": {
            "and": [
                {
                    "property": MONTHLY_DB_TITLE_PROP,
                    "title": {"equals": month_text}
                },
                {
                    "property": MONTHLY_DB_YEAR_PROP,
                    "number": {"equals": year}
                }
            ]
        }
    }

    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        
        if response.status_code != 200:
            print(f"⚠️ Error searching monthly page: {response.text}")
            return None

        results = response.json().get("results", [])
        
        if results:
            page_id = results[0]["id"]
            print(f"  ✅ Found monthly page: {month_text} (Year: {year})")
            return page_id
        else:
            print(f"  ⚠️ No monthly page found with Month='{month_text}' and Year={year}")
            return None
            
    except Exception as e:
        print(f"❌ Exception finding monthly page: {e}")
        return None


def update_task_relations(task_id, weekly_page_id, monthly_page_id):
    """
    Updates the 'Weekly Link' and 'Monthly Link' relation properties of a task.
    """
    url = f"https://api.notion.com/v1/pages/{task_id}"
    
    properties_to_update = {}

    if weekly_page_id:
        properties_to_update[TASK_PROP_WEEKLY_LINK] = {
            "relation": [{"id": weekly_page_id}]
        }
    if monthly_page_id:
        properties_to_update[TASK_PROP_MONTHLY_LINK] = {
            "relation": [{"id": monthly_page_id}]
        }

    if not properties_to_update:
        print(f"  - No summary pages found for task {task_id}, skipping update.")
        return

    payload = {"properties": properties_to_update}

    try:
        print(f"  - Updating relations for task {task_id}...")
        response = requests.patch(url, headers=HEADERS, json=payload)
        
        if response.status_code == 200:
            print(f"  - ✅ Successfully linked task {task_id}.")
        else:
            print(f"❌ Failed to update task {task_id}: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception updating task {task_id}: {e}")


def main():
    """
    Main execution function to orchestrate the automation.
    """
    tasks_to_process = get_unlinked_tasks()

    if not tasks_to_process:
        print("No new tasks to process. Exiting.")
        return

    print(f"Found {len(tasks_to_process)} task(s) to process.")
    print("🗓️  Using Year + Text matching (Week/Month stay as text!)\n")

    for task in tasks_to_process:
        task_id = task.get("id")
        properties = task.get("properties", {})

        try:
            # Handle Title
            if properties.get(TASK_PROP_TITLE, {}).get("title"):
                task_title = properties[TASK_PROP_TITLE]["title"][0]["plain_text"]
            else:
                task_title = "Untitled Task"

            # Extract year (number), week (text), and month (text)
            year, week_text, month_text = extract_task_properties(properties)
            
            if not all([year, week_text, month_text]):
                print(f"⏩ Skipping task '{task_title}' ({task_id}) - Missing required properties")
                continue

        except (KeyError, IndexError, TypeError) as e:
            print(f"⏩ Skipping task {task_id} due to property error: {e}")
            continue

        print(f"\n📋 Processing task: '{task_title}' (ID: {task_id})")
        print(f"  📅 Year: {year}, Week: '{week_text}', Month: '{month_text}'")

        # Find the corresponding weekly summary page (matching BOTH week text AND year)
        weekly_page_id = find_weekly_page_with_year(week_text, year)

        # Find the corresponding monthly summary page (matching BOTH month text AND year)
        monthly_page_id = find_monthly_page_with_year(month_text, year)

        # Update the task with the new relations
        update_task_relations(task_id, weekly_page_id, monthly_page_id)


if __name__ == "__main__":
    main()
