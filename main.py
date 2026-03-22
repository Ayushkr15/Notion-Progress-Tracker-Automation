import os
import sys
import time
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# --- ⚙️ Configuration ---
TASK_PROP_DUE_DATE = "Due Date"
TASK_PROP_TITLE = "Tasks"
TASK_PROP_WEEKLY_LINK = "Weekly Link"
TASK_PROP_MONTHLY_LINK = "Monthly Link"
TASK_PROP_WEEK_NUMBER = "Week Number"   # Returns 41 (number or text)
TASK_PROP_MONTH = "Month"               # Returns "October" (text)
TASK_PROP_YEAR = "Year"                 # Returns 2025 (number)

# Weekly Progress Database
WEEKLY_DB_TITLE_PROP = "Week Number"    # Title property
WEEKLY_DB_YEAR_PROP = "Year"            # Number property

# Monthly Progress Database
MONTHLY_DB_TITLE_PROP = "Month"         # Title property
MONTHLY_DB_YEAR_PROP = "Year"           # Number property

# Retry / rate-limit settings
MAX_RETRIES = 3
INITIAL_BACKOFF_S = 1.0
RATE_LIMIT_SLEEP_S = 0.35              # ~3 req/s Notion limit

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

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _request_with_retry(method, url, **kwargs):
    """Make an HTTP request with retries + exponential backoff.
    
    Retries on 429 (rate-limit) and 5xx errors.
    Returns the Response object, or None on total failure.
    """
    backoff = INITIAL_BACKOFF_S
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(RATE_LIMIT_SLEEP_S)  # rate-limit pacing
            resp = requests.request(method, url, headers=HEADERS, **kwargs)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", backoff))
                print(f"  ⏳ Rate-limited. Waiting {retry_after}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(retry_after)
                backoff *= 2
                continue

            if resp.status_code >= 500:
                print(f"  ⚠️ Server error {resp.status_code}. Retrying in {backoff}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(backoff)
                backoff *= 2
                continue

            return resp  # success or client error we shouldn't retry
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ Network error: {e}. Retrying in {backoff}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(backoff)
            backoff *= 2

    print("  ❌ All retries exhausted.")
    return None


def _paginated_query(database_id, payload):
    """Query a Notion database with automatic pagination.
    
    Returns the full list of result pages.
    """
    all_results = []
    has_more = True
    start_cursor = None

    while has_more:
        body = dict(payload)
        if start_cursor:
            body["start_cursor"] = start_cursor

        resp = _request_with_retry("POST",
                                   f"https://api.notion.com/v1/databases/{database_id}/query",
                                   json=body)
        if resp is None or resp.status_code != 200:
            error_text = resp.text if resp else "No response"
            print(f"  ❌ Query failed: {error_text}")
            break

        data = resp.json()
        all_results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return all_results


# ──────────────────────────────────────────────
# Core Functions
# ──────────────────────────────────────────────

def get_backfill_tasks():
    """Get ALL tasks that have a Due Date but are missing Weekly Link OR Monthly Link.
    
    No time filter — this catches every historical gap.
    """
    payload = {
        "filter": {
            "and": [
                {"property": TASK_PROP_DUE_DATE, "date": {"is_not_empty": True}},
                {
                    "or": [
                        {"property": TASK_PROP_WEEKLY_LINK, "relation": {"is_empty": True}},
                        {"property": TASK_PROP_MONTHLY_LINK, "relation": {"is_empty": True}},
                    ]
                }
            ]
        }
    }
    print("🔎 [Backfill] Querying for ALL unlinked tasks...")
    results = _paginated_query(TASKS_DB_ID, payload)
    print(f"   Found {len(results)} task(s) in backfill sweep.")
    return results


def get_incremental_tasks():
    """Get recently-edited tasks that are missing Weekly Link OR Monthly Link.
    
    Uses a 65-minute lookback window (matches the hourly cron + margin).
    """
    cut_off_time = (datetime.now(timezone.utc) - timedelta(minutes=65)).isoformat()
    payload = {
        "filter": {
            "and": [
                {"property": TASK_PROP_DUE_DATE, "date": {"is_not_empty": True}},
                {
                    "or": [
                        {"property": TASK_PROP_WEEKLY_LINK, "relation": {"is_empty": True}},
                        {"property": TASK_PROP_MONTHLY_LINK, "relation": {"is_empty": True}},
                    ]
                },
                {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": cut_off_time}}
            ]
        }
    }
    print("🔎 [Incremental] Querying for recently-edited unlinked tasks...")
    results = _paginated_query(TASKS_DB_ID, payload)
    print(f"   Found {len(results)} task(s) in incremental sweep.")
    return results


def merge_tasks(backfill, incremental):
    """Merge two task lists, de-duplicating by task ID."""
    seen = {}
    for task in backfill + incremental:
        tid = task.get("id")
        if tid and tid not in seen:
            seen[tid] = task
    return list(seen.values())


def extract_task_properties(task_properties):
    """Extract year, week, month from task formula properties."""
    try:
        # Extract Year (number)
        year_prop = task_properties.get(TASK_PROP_YEAR, {}).get("formula", {})
        if year_prop.get("type") == "number":
            year = year_prop.get("number")
            if year is None:
                return None, None, None
            year = int(year)
        else:
            return None, None, None

        # Extract Week Number (could be text "41" or number 41)
        week_prop = task_properties.get(TASK_PROP_WEEK_NUMBER, {}).get("formula", {})
        week_value = week_prop.get("string") or week_prop.get("number")
        if not week_value:
            return None, None, None
        week_text = str(week_value)

        # Extract Month (text like "October")
        month_prop = task_properties.get(TASK_PROP_MONTH, {}).get("formula", {})
        month_text = month_prop.get("string")
        if not month_text:
            return None, None, None

        return year, week_text, month_text

    except Exception as e:
        print(f"  ⚠️ Error extracting properties: {e}")
        return None, None, None


def _has_existing_relation(task_properties, relation_prop_name):
    """Check if a relation property already has at least one linked page."""
    rel = task_properties.get(relation_prop_name, {})
    rel_list = rel.get("relation", [])
    return len(rel_list) > 0


def _create_weekly_page(week_text, year):
    """Create a new Weekly Progress page with the given week number and year."""
    payload = {
        "parent": {"database_id": WEEKLY_DB_ID},
        "properties": {
            WEEKLY_DB_TITLE_PROP: {
                "title": [{"text": {"content": week_text}}]
            },
            WEEKLY_DB_YEAR_PROP: {
                "number": year
            }
        }
    }
    resp = _request_with_retry("POST",
                               "https://api.notion.com/v1/pages",
                               json=payload)
    if resp is None or resp.status_code != 200:
        error_text = resp.text if resp else "No response"
        print(f"  ❌ Failed to create Weekly page: {error_text}")
        return None

    page_id = resp.json().get("id")
    print(f"  🆕 Created: Week {week_text} ({year})")
    return page_id


def _create_monthly_page(month_text, year):
    """Create a new Monthly Progress page with the given month name and year."""
    payload = {
        "parent": {"database_id": MONTHLY_DB_ID},
        "properties": {
            MONTHLY_DB_TITLE_PROP: {
                "title": [{"text": {"content": month_text}}]
            },
            MONTHLY_DB_YEAR_PROP: {
                "number": year
            }
        }
    }
    resp = _request_with_retry("POST",
                               "https://api.notion.com/v1/pages",
                               json=payload)
    if resp is None or resp.status_code != 200:
        error_text = resp.text if resp else "No response"
        print(f"  ❌ Failed to create Monthly page: {error_text}")
        return None

    page_id = resp.json().get("id")
    print(f"  🆕 Created: {month_text} {year}")
    return page_id


def find_weekly_page(week_text, year, auto_create=True):
    """Find Weekly Progress page by week number AND year.
    
    If not found and auto_create is True, creates the page automatically.
    Returns (page_id, was_created) tuple.
    """
    payload = {
        "filter": {
            "and": [
                {"property": WEEKLY_DB_TITLE_PROP, "title": {"equals": week_text}},
                {"property": WEEKLY_DB_YEAR_PROP, "number": {"equals": year}},
            ]
        }
    }
    resp = _request_with_retry("POST",
                               f"https://api.notion.com/v1/databases/{WEEKLY_DB_ID}/query",
                               json=payload)
    if resp is None or resp.status_code != 200:
        error_text = resp.text if resp else "No response"
        print(f"  ⚠️ Error searching weekly: {error_text}")
        return None, False

    results = resp.json().get("results", [])
    if results:
        print(f"  ✅ Found: Week {week_text} ({year})")
        return results[0]["id"], False

    # Page not found — auto-create if enabled
    if auto_create:
        print(f"  ⚠️ Not found: Week {week_text}, Year {year} → Auto-creating...")
        page_id = _create_weekly_page(week_text, year)
        return page_id, (page_id is not None)
    else:
        print(f"  ⚠️ Not found: Week {week_text}, Year {year}")
        return None, False


def find_monthly_page(month_text, year, auto_create=True):
    """Find Monthly Progress page by month name AND year.
    
    If not found and auto_create is True, creates the page automatically.
    Returns (page_id, was_created) tuple.
    """
    # Validate month name before searching
    valid_months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    if month_text not in valid_months:
        print(f"  ⚠️ Invalid month name: '{month_text}' — skipping")
        return None, False

    payload = {
        "filter": {
            "and": [
                {"property": MONTHLY_DB_TITLE_PROP, "title": {"equals": month_text}},
                {"property": MONTHLY_DB_YEAR_PROP, "number": {"equals": year}},
            ]
        }
    }
    resp = _request_with_retry("POST",
                               f"https://api.notion.com/v1/databases/{MONTHLY_DB_ID}/query",
                               json=payload)
    if resp is None or resp.status_code != 200:
        error_text = resp.text if resp else "No response"
        print(f"  ⚠️ Error searching monthly: {error_text}")
        return None, False

    results = resp.json().get("results", [])
    if results:
        print(f"  ✅ Found: {month_text} {year}")
        return results[0]["id"], False

    # Page not found — auto-create if enabled
    if auto_create:
        print(f"  ⚠️ Not found: {month_text}, Year {year} → Auto-creating...")
        page_id = _create_monthly_page(month_text, year)
        return page_id, (page_id is not None)
    else:
        print(f"  ⚠️ Not found: {month_text}, Year {year}")
        return None, False


def update_task_relations(task_id, weekly_page_id, monthly_page_id):
    """Update task relation properties. Returns True on success."""
    properties_to_update = {}
    if weekly_page_id:
        properties_to_update[TASK_PROP_WEEKLY_LINK] = {"relation": [{"id": weekly_page_id}]}
    if monthly_page_id:
        properties_to_update[TASK_PROP_MONTHLY_LINK] = {"relation": [{"id": monthly_page_id}]}

    if not properties_to_update:
        return True  # nothing to do

    resp = _request_with_retry("PATCH",
                               f"https://api.notion.com/v1/pages/{task_id}",
                               json={"properties": properties_to_update})
    if resp is None or resp.status_code != 200:
        error_text = resp.text if resp else "No response"
        print(f"  ❌ Failed to update: {error_text}")
        return False

    print(f"  ✅ Successfully linked task")
    return True


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    """Two-phase sync: backfill all gaps, then incremental recent tasks."""
    # Phase 1 — Backfill
    backfill_tasks = get_backfill_tasks()

    # Phase 2 — Incremental (recent edits)
    incremental_tasks = get_incremental_tasks()

    # Merge & de-duplicate
    tasks_to_process = merge_tasks(backfill_tasks, incremental_tasks)
    print(f"\n📋 Total unique tasks to process: {len(tasks_to_process)}")
    print(f"   (Backfill: {len(backfill_tasks)}, Incremental: {len(incremental_tasks)})")
    print(f"🗓️  Using Year-aware matching\n")

    if not tasks_to_process:
        print("✅ No tasks to process. Everything is in sync!")
        return

    # Counters
    linked = 0
    skipped = 0
    failed = 0
    created_weekly = 0
    created_monthly = 0

    for task in tasks_to_process:
        task_id = task.get("id")
        properties = task.get("properties", {})

        try:
            # Get title
            title_list = properties.get(TASK_PROP_TITLE, {}).get("title", [])
            task_title = title_list[0]["plain_text"] if title_list else "Untitled Task"

            # Get year, week, month
            year, week_text, month_text = extract_task_properties(properties)

            if not all([year, week_text, month_text]):
                print(f"⏩ Skipping '{task_title}' — Missing Year/Week/Month properties")
                skipped += 1
                continue

        except Exception as e:
            print(f"⏩ Skipping task — Error reading properties: {e}")
            skipped += 1
            continue

        print(f"\n📋 Processing: '{task_title}'")
        print(f"   📅 Year: {year}, Week: {week_text}, Month: {month_text}")

        # Check which relations are already set
        needs_weekly = not _has_existing_relation(properties, TASK_PROP_WEEKLY_LINK)
        needs_monthly = not _has_existing_relation(properties, TASK_PROP_MONTHLY_LINK)

        if not needs_weekly and not needs_monthly:
            print(f"  ⏩ Already fully linked, skipping")
            skipped += 1
            continue

        # Find pages only for the missing relations (auto-creates if not found)
        weekly_page_id, weekly_was_created = find_weekly_page(week_text, year) if needs_weekly else (None, False)
        monthly_page_id, monthly_was_created = find_monthly_page(month_text, year) if needs_monthly else (None, False)

        if weekly_was_created:
            created_weekly += 1
        if monthly_was_created:
            created_monthly += 1

        # If we couldn't find or create the target pages, skip
        if needs_weekly and not weekly_page_id:
            print(f"  ⚠️ Cannot link weekly — page not found or created")
        if needs_monthly and not monthly_page_id:
            print(f"  ⚠️ Cannot link monthly — page not found or created")

        if not weekly_page_id and not monthly_page_id:
            print(f"  ⏩ No matching pages found, skipping")
            skipped += 1
            continue

        # Update task
        success = update_task_relations(task_id, weekly_page_id, monthly_page_id)
        if success:
            linked += 1
        else:
            failed += 1

    # Summary
    print("\n" + "═" * 45)
    print(f"✨ Sync Complete!")
    print(f"   ✅ Linked:   {linked}")
    print(f"   🆕 Created:  {created_weekly} weekly, {created_monthly} monthly pages")
    print(f"   ⏩ Skipped:  {skipped}")
    print(f"   ❌ Failed:   {failed}")
    print("═" * 45)

    if failed > 0:
        print(f"\n⚠️ {failed} task(s) failed. They will be retried on the next run.")
        sys.exit(1)


if __name__ == "__main__":
    main()
