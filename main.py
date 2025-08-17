import os
import sys
from datetime import datetime, timedelta, timezone
from notion_client import Client
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

# --- ⚙️ Configuration ---
# Set these to the EXACT names of your Notion properties (case-sensitive)
TASK_PROP_DUE_DATE = "Due Date"
TASK_PROP_TITLE = "Tasks"
TASK_PROP_WEEKLY_LINK = "Weekly Link"
TASK_PROP_MONTHLY_LINK = "Monthly Link"
TASK_PROP_WEEK_NUMBER = "Week Number"
TASK_PROP_MONTH_TEXT = "Month"

# Title property names in your "Weekly Progress" and "Monthly Progress" databases
WEEKLY_DB_TITLE_PROP = "Week Number"
MONTHLY_DB_TITLE_PROP = "Month"

# --- Secrets & Initialization ---
# These are loaded from your .env file locally or from GitHub Secrets when automated
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
TASKS_DB_ID = os.getenv("TASKS_DB_ID")
WEEKLY_DB_ID = os.getenv("WEEKLY_DB_ID")
MONTHLY_DB_ID = os.getenv("MONTHLY_DB_ID")

# Verify that all necessary secrets have been loaded
if not all([NOTION_API_KEY, TASKS_DB_ID, WEEKLY_DB_ID, MONTHLY_DB_ID]):
    print("❌ ERROR: Missing one or more required environment variables.")
    sys.exit(1)

# Initialize the Notion API client
notion = Client(auth=NOTION_API_KEY)


def get_unlinked_tasks():
    """
    Queries for tasks that were recently edited, have a due date, 
    and are not yet linked to a weekly report.
    """
    # Calculate a timestamp for 65 minutes ago to only fetch recent changes
    # This buffer prevents missing tasks due to minor timing differences
    cut_off_time = (datetime.now(timezone.utc) -
                    timedelta(minutes=65)).isoformat()

    try:
        print("🔎 Querying for unlinked tasks...")
        response = notion.databases.query(
            database_id=TASKS_DB_ID,
            filter={
                "and": [
                    {"property": TASK_PROP_DUE_DATE,
                        "date": {"is_not_empty": True}},
                    {"property": TASK_PROP_WEEKLY_LINK,
                        "relation": {"is_empty": True}},
                    # This filter makes the automation efficient by only checking recent tasks
                    {"timestamp": "last_edited_time",
                        "last_edited_time": {"on_or_after": cut_off_time}}
                ]
            }
        )
        return response.get("results", [])
    except Exception as e:
        print(f"❌ Error querying tasks database: {e}")
        return []


def find_summary_page(database_id, title_property_name, query_value):
    """Searches a database for a page whose title exactly matches the query."""
    try:
        response = notion.databases.query(
            database_id=database_id,
            filter={"property": title_property_name,
                    "title": {"equals": str(query_value)}}
        )
        results = response.get("results", [])
        if results:
            # Return the ID of the first matching page
            return results[0]["id"]
        else:
            print(f"  - No summary page found with title: '{query_value}'")
            return None
    except Exception as e:
        print(f"❌ Error finding summary page '{query_value}': {e}")
        return None


def update_task_relations(task_id, weekly_page_id, monthly_page_id):
    """Updates the 'Weekly Link' and 'Monthly Link' relation properties of a task."""
    properties_to_update = {}

    if weekly_page_id:
        properties_to_update[TASK_PROP_WEEKLY_LINK] = {
            "relation": [{"id": weekly_page_id}]}
    if monthly_page_id:
        properties_to_update[TASK_PROP_MONTHLY_LINK] = {
            "relation": [{"id": monthly_page_id}]}

    # Skip the API call if no summary pages were found to link
    if not properties_to_update:
        print(
            f"  - No summary pages found for task {task_id}, skipping update.")
        return

    try:
        print(f"  - Updating relations for task {task_id}...")
        notion.pages.update(page_id=task_id, properties=properties_to_update)
        print(f"  - ✅ Successfully linked task {task_id}.")
    except Exception as e:
        print(f"❌ Error updating task {task_id}: {e}")


def main():
    """Main execution function to orchestrate the automation."""
    tasks_to_process = get_unlinked_tasks()

    if not tasks_to_process:
        print("No new tasks to process. Exiting.")
        return

    print(f"Found {len(tasks_to_process)} task(s) to process.")

    for task in tasks_to_process:
        task_id = task.get("id")
        properties = task.get("properties", {})

        try:
            # Extract the required values from the task's properties
            task_title = properties[TASK_PROP_TITLE]["title"][0]["plain_text"]
            week_number = properties[TASK_PROP_WEEK_NUMBER]["formula"]["string"]
            month_name = properties[TASK_PROP_MONTH_TEXT]["formula"]["string"]

        except (KeyError, IndexError, TypeError):
            # Skip any tasks that are missing the required formula properties
            print(
                f"⏩ Skipping task {task_id} due to missing or incorrect properties.")
            continue

        print(f"\nProcessing task: '{task_title}' (ID: {task_id})")

        # Find the ID of the corresponding weekly summary page
        weekly_page_id = find_summary_page(
            WEEKLY_DB_ID, WEEKLY_DB_TITLE_PROP, week_number)

        # Find the ID of the corresponding monthly summary page
        monthly_page_id = find_summary_page(
            MONTHLY_DB_ID, MONTHLY_DB_TITLE_PROP, month_name)

        # Update the original task with the new relations
        update_task_relations(task_id, weekly_page_id, monthly_page_id)


# Standard entry point for a Python script
if __name__ == "__main__":
    main()
