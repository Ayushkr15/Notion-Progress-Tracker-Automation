import os
import sys
import requests
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# --- ⚙️ Configuration ---
TASK_PROP_DUE_DATE = "Due Date"
TASK_PROP_TITLE = "Tasks"
TASK_PROP_WEEKLY_LINK = "Weekly Link"
TASK_PROP_MONTHLY_LINK = "Monthly Link"
TASK_PROP_WEEK_NUMBER = "Week Number"  # Returns 41 (number or text)
TASK_PROP_MONTH = "Month"  # Returns "October" (text)
TASK_PROP_YEAR = "Year"  # Returns 2025 (number)

# Weekly Progress Database
WEEKLY_DB_TITLE_PROP = "Week Number"  # Title property
WEEKLY_DB_YEAR_PROP = "Year"  # NEW: Number property you just added

# Monthly Progress Database
MONTHLY_DB_TITLE_PROP = "Month"  # Title property
MONTHLY_DB_YEAR_PROP = "Year"  # NEW: Number property you just added

# --- Secrets ---
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
TASKS_DB_ID = os.getenv("TASKS_DB_ID")
WEEKLY_DB_ID = os.getenv("WEEKLY_DB_ID")
MONTHLY_DB_ID = os.getenv("MONTHLY_DB_ID")

if not all([NOTION_API_KEY, TASKS_DB_ID, WEEKLY_DB_ID, MONTHLY_DB_ID]):
    print("❌ ERROR: Missing environment variables.")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_unlinked_tasks():
    """Query for unlinked tasks."""
    cut_off_time = (datetime.now(timezone.utc) - timedelta(minutes=65)).isoformat()

    url = f"https://api.notion.com/v1/databases/{TASKS_DB_ID}/query"
    payload = {
        "filter": {
            "and": [
                {"property": TASK_PROP_DUE_DATE, "date": {"is_not_empty": True}},
                {"property": TASK_PROP_WEEKLY_LINK, "relation": {"is_empty": True}},
                {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": cut_off_time}}
            ]
        }
    }

    try:
        print("🔎 Querying for unlinked tasks...")
        response = requests.post(url, headers=HEADERS, json=payload)
        if response.status_code != 200:
            print(f"❌ Error: {response.text}")
            return []
        return response.json().get("results", [])
    except Exception as e:
        print(f"❌ Exception: {e}")
        return []


def extract_task_properties(task_properties):
    """Extract year, week, month from task."""
    try:
        # Extract Year (number)
        year_prop = task_properties.get(TASK_PROP_YEAR, {}).get("formula", {})
        if year_prop.get("type") == "number":
            year = int(year_prop.get("number", 0))
        else:
            return None, None, None

        # Extract Week Number (could be text "41" or number 41)
        week_prop = task_properties.get(TASK_PROP_WEEK_NUMBER, {}).get("formula", {})
        week_value = week_prop.get("string") or week_prop.get("number")
        if not week_value:
            return None, None, None
        week_text = str(week_value)  # Convert to text for matching

        # Extract Month (text like "October")
        month_prop = task_properties.get(TASK_PROP_MONTH, {}).get("formula", {})
        month_text = month_prop.get("string")
        if not month_text:
            return None, None, None

        return year, week_text, month_text

    except Exception as e:
        print(f"  ⚠️ Error extracting properties: {e}")
        return None, None, None


def find_weekly_page(week_text, year):
    """Find Weekly Progress page by week number AND year."""
    url = f"https://api.notion.com/v1/databases/{WEEKLY_DB_ID}/query"
    
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
            print(f"⚠️ Error searching weekly: {response.text}")
            return None

        results = response.json().get("results", [])
        if results:
            print(f"  ✅ Found: Week {week_text} ({year})")
            return results[0]["id"]
        else:
            print(f"  ⚠️ Not found: Week {week_text}, Year {year}")
            return None
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None


def find_monthly_page(month_text, year):
    """Find Monthly Progress page by month name AND year."""
    url = f"https://api.notion.com/v1/databases/{MONTHLY_DB_ID}/query"
    
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
            print(f"⚠️ Error searching monthly: {response.text}")
            return None

        results = response.json().get("results", [])
        if results:
            print(f"  ✅ Found: {month_text} {year}")
            return results[0]["id"]
        else:
            print(f"  ⚠️ Not found: {month_text}, Year {year}")
            return None
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None


def update_task_relations(task_id, weekly_page_id, monthly_page_id):
    """Update task relations."""
    url = f"https://api.notion.com/v1/pages/{task_id}"
    
    properties_to_update = {}
    if weekly_page_id:
        properties_to_update[TASK_PROP_WEEKLY_LINK] = {"relation": [{"id": weekly_page_id}]}
    if monthly_page_id:
        properties_to_update[TASK_PROP_MONTHLY_LINK] = {"relation": [{"id": monthly_page_id}]}

    if not properties_to_update:
        return

    payload = {"properties": properties_to_update}

    try:
        response = requests.patch(url, headers=HEADERS, json=payload)
        if response.status_code == 200:
            print(f"  ✅ Successfully linked task")
        else:
            print(f"❌ Failed: {response.text}")
    except Exception as e:
        print(f"❌ Exception: {e}")


def main():
    """Main function."""
    tasks_to_process = get_unlinked_tasks()

    if not tasks_to_process:
        print("✅ No new tasks to process.")
        return

    print(f"📋 Found {len(tasks_to_process)} task(s) to process.")
    print(f"🗓️  Using Year-aware matching\n")

    for task in tasks_to_process:
        task_id = task.get("id")
        properties = task.get("properties", {})

        try:
            # Get title
            if properties.get(TASK_PROP_TITLE, {}).get("title"):
                task_title = properties[TASK_PROP_TITLE]["title"][0]["plain_text"]
            else:
                task_title = "Untitled Task"

            # Get year, week, month
            year, week_text, month_text = extract_task_properties(properties)
            
            if not all([year, week_text, month_text]):
                print(f"⏩ Skipping '{task_title}' - Missing properties")
                continue

        except Exception as e:
            print(f"⏩ Skipping task - Error: {e}")
            continue

        print(f"\n📋 Processing: '{task_title}'")
        print(f"  📅 Year: {year}, Week: {week_text}, Month: {month_text}")

        # Find pages
        weekly_page_id = find_weekly_page(week_text, year)
        monthly_page_id = find_monthly_page(month_text, year)

        # Update task
        update_task_relations(task_id, weekly_page_id, monthly_page_id)

    print("\n✨ Complete!")


if __name__ == "__main__":
    main()
