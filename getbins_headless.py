#!/usr/bin/env python3
"""
Headless script to fetch bin collection dates from Bath & North East Somerset Council.
Uses Playwright for headless browser automation and BeautifulSoup for HTML parsing.
"""

import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("Warning: python-dotenv not installed. .env file will not be loaded.", file=sys.stderr)
    load_dotenv = None

URL = "https://app.bathnes.gov.uk/webforms/waste/collectionday/"

# Load environment variables from .env file if it exists
if load_dotenv:
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✓ Loaded environment variables from {env_path}", file=sys.stderr)
    else:
        # Also try loading from current directory
        load_dotenv()

# Get configuration from environment variables
POSTCODE = os.getenv("POSTCODE", "").strip().strip('"\'')
ADDRESS_TEXT = os.getenv("ADDRESS_LINE", "").strip().strip('"\'')
TIMEZONE = os.getenv("TIMEZONE", "Europe/London").strip().strip('"\'')

# Database configuration from environment variables
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "").strip().strip('"\''),
    "port": int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DATABASE", "binday").strip().strip('"\''),
    "user": os.getenv("PG_USERNAME", "").strip().strip('"\''),
    "password": os.getenv("PG_PASSWORD", "").strip().strip('"\''),
    "application_name": os.getenv("PG_APPNAME", "binday-scraper").strip().strip('"\'')
}

# Validate required environment variables
if not POSTCODE:
    print("Error: POSTCODE environment variable is required", file=sys.stderr)
    sys.exit(1)

if not ADDRESS_TEXT:
    print("Error: ADDRESS_LINE environment variable is required", file=sys.stderr)
    sys.exit(1)

if not DB_CONFIG["host"]:
    print("Error: PG_HOST environment variable is required", file=sys.stderr)
    sys.exit(1)

if not DB_CONFIG["user"]:
    print("Error: PG_USERNAME environment variable is required", file=sys.stderr)
    sys.exit(1)

if not DB_CONFIG["password"]:
    print("Error: PG_PASSWORD environment variable is required", file=sys.stderr)
    sys.exit(1)


def random_wait():
    """Wait for a random time between 0.5 and 2 seconds."""
    time.sleep(random.uniform(0.5, 2.0))


def get_waste_group(collection_type):
    """Map collection type to waste group(s).
    
    Args:
        collection_type: The collection type string
    
    Returns:
        String or list of strings representing the waste group(s)
    """
    if not collection_type:
        return None
    
    collection_type_lower = collection_type.lower()
    
    # Black Rubbish Bin
    if "black" in collection_type_lower and "rubbish" in collection_type_lower:
        return "General Rubbish (black bin)"
    
    # Blue Recycling Bag for Cardboard
    if "blue" in collection_type_lower and ("cardboard" in collection_type_lower or "bag" in collection_type_lower):
        return "Cardboard (blue bag/box)"
    
    # Food Recycling Collection Bin
    if "food" in collection_type_lower or "caddy" in collection_type_lower:
        return "Food Waste (caddy)"
    
    # Green Recycling Box
    if "green" in collection_type_lower and "recycling" in collection_type_lower:
        return [
            "Plastics & Metals (green box)",
            "Glass & Paper (green box)"
        ]
    
    # Garden Waste Bin
    if "garden" in collection_type_lower and "waste" in collection_type_lower:
        return "Garden Waste (garden bin subscription)"
    
    # Default fallback
    return None


def is_date(val):
    """Check if a value looks like a date."""
    if not val:
        return False
    return any(day in val for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])


def parse_collection_date(date_str):
    """Parse a collection date string into a datetime object at 7am local time.
    
    Args:
        date_str: Date string like "Monday, 17 November 2025"
    
    Returns:
        datetime object at 7am in Europe/London timezone, or None if parsing fails
    """
    if not date_str:
        return None
    
    try:
        # Parse date string (format: "Monday, 17 November 2025")
        # Remove day name and comma
        date_part = date_str.split(',', 1)[-1].strip()
        # Parse the date
        dt = datetime.strptime(date_part, "%d %B %Y")
        # Set time to 7am
        dt = dt.replace(hour=7, minute=0, second=0, microsecond=0)
        # Set timezone to configured timezone
        try:
            tz = ZoneInfo(TIMEZONE)
        except Exception:
            tz = ZoneInfo("Europe/London")
        dt = dt.replace(tzinfo=tz)
        return dt
    except (ValueError, AttributeError):
        return None


def format_time_until_next(days, minutes):
    """Format time until next collection as plain text.
    
    Args:
        days: Number of days
        minutes: Total number of minutes
    
    Returns:
        Plain text string like "2 days, 5 hours and 30 minutes"
    """
    if minutes < 0:
        return "Collection time has passed"
    
    # Calculate hours and remaining minutes
    total_minutes = minutes
    hours = total_minutes // 60
    remaining_minutes = total_minutes % 60
    
    # If we have days, subtract the minutes already accounted for in days
    if days > 0:
        hours_in_days = days * 24
        hours = hours - hours_in_days
    
    parts = []
    
    if days > 0:
        if days == 1:
            parts.append("1 day")
        else:
            parts.append(f"{days} days")
    
    if hours > 0:
        if hours == 1:
            parts.append("1 hour")
        else:
            parts.append(f"{hours} hours")
    
    if remaining_minutes > 0:
        if remaining_minutes == 1:
            parts.append("1 minute")
        else:
            parts.append(f"{remaining_minutes} minutes")
    
    if not parts:
        return "Less than 1 minute"
    
    # Format with proper conjunctions
    if len(parts) == 1:
        return parts[0]
    elif len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    else:
        return ", ".join(parts[:-1]) + f" and {parts[-1]}"


def calculate_time_differences(next_collection_str, last_collection_str):
    """Calculate days and minutes since last collection and until next collection.
    
    Args:
        next_collection_str: Next collection date string
        last_collection_str: Last collection date string
    
    Returns:
        Dictionary with time difference calculations
    """
    result = {}
    
    # Get current time in the configured timezone
    try:
        tz = ZoneInfo(TIMEZONE)
    except Exception:
        print(f"Warning: Invalid timezone '{TIMEZONE}', using Europe/London", file=sys.stderr)
        tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)
    
    # Calculate time until next collection
    if next_collection_str:
        next_dt = parse_collection_date(next_collection_str)
        if next_dt:
            if next_dt >= now:
                # Next collection is in the future
                delta = next_dt - now
                days = delta.days
                minutes = int(delta.total_seconds() / 60)
                result["days_until_next"] = days
                result["minutes_until_next"] = minutes
                # Add plain text representation
                result["time_until_next_text"] = format_time_until_next(days, minutes)
            else:
                # Next collection is in the past (shouldn't happen, but handle it)
                result["days_until_next"] = 0
                result["minutes_until_next"] = 0
                result["time_until_next_text"] = "Collection time has passed"
    
    # Calculate time since last collection
    if last_collection_str:
        last_dt = parse_collection_date(last_collection_str)
        if last_dt:
            if last_dt <= now:
                # Last collection is in the past
                delta = now - last_dt
                days = delta.days
                minutes = int(delta.total_seconds() / 60)
                result["days_since_last"] = days
                result["minutes_since_last"] = minutes
            else:
                # Last collection is in the future (shouldn't happen, but handle it)
                result["days_since_last"] = 0
                result["minutes_since_last"] = 0
    
    return result


def parse_collection_table(html):
    """Parse the collection dates table from HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find the table with collection dates
    table = soup.find('table')
    if not table:
        return []
    
    # Get headers
    header_cells = table.find('thead')
    headers = []
    if header_cells:
        headers = [th.get_text(strip=True) for th in header_cells.find_all('th')]
    
    # Find column indices
    next_col_idx = None
    last_col_idx = None
    collection_type_idx = None
    
    for i, header in enumerate(headers):
        if "Next collection" in header:
            next_col_idx = i
        elif "Last collection" in header:
            last_col_idx = i
        elif "Collection" in header and header != "":
            collection_type_idx = i
    
    # Parse rows
    rows = table.find('tbody')
    if not rows:
        return []
    
    collections = []
    
    for row in rows.find_all('tr'):
        # Check for row header (collection type)
        row_header = row.find('th')
        collection_type_from_header = None
        if row_header:
            collection_type_from_header = row_header.get_text(strip=True)
        
        # Get data cells
        cells = row.find_all('td')
        values = [cell.get_text(strip=True) for cell in cells]
        
        collection_item = {}
        
        # Extract collection type
        collection_type = None
        if row_header:
            # Get text from row header, preserving line breaks
            collection_type = row_header.get_text(separator=' | ', strip=True)
        elif collection_type_idx is not None and collection_type_idx < len(values):
            val = values[collection_type_idx] if values[collection_type_idx] else None
            if val and not is_date(val):
                collection_type = val
        elif len(values) > 0:
            if values[0] and not is_date(values[0]):
                collection_type = values[0]
        
        if collection_type:
            # Clean up collection type - replace multiple spaces with single space
            collection_type = ' '.join(collection_type.split())
        
        # Extract dates first (we'll use these for all split types)
        date_values = []
        for val in values:
            if val and is_date(val):
                date_values.append(val)
        
        # Map dates using header indices
        next_collection = None
        last_collection = None
        
        if next_col_idx is not None and next_col_idx < len(values):
            val = values[next_col_idx] if values[next_col_idx] else None
            if val and is_date(val):
                next_collection = val
        
        if last_col_idx is not None and last_col_idx < len(values):
            val = values[last_col_idx] if values[last_col_idx] else None
            if val and is_date(val):
                last_collection = val
        
        # Fallback: use positional
        if not next_collection and date_values:
            next_collection = date_values[0]
        
        if not last_collection and len(date_values) >= 2:
            last_collection = date_values[1]
        
        # Split collection type if it contains " | " separator
        # Note: Time differences are calculated on-the-fly by applications, not stored
        if collection_type and " | " in collection_type:
            # Split into individual collection types
            individual_types = [ct.strip() for ct in collection_type.split(" | ")]
            # Create a separate item for each type with the same dates
            for individual_type in individual_types:
                if individual_type:  # Only add non-empty types
                    item = {
                        "collection_type": individual_type,
                    }
                    # Add waste group
                    waste_group = get_waste_group(individual_type)
                    if waste_group:
                        item["waste_group"] = waste_group
                    if next_collection:
                        item["next_collection"] = next_collection
                    if last_collection:
                        item["last_collection"] = last_collection
                    # Calculate time differences for JSON output (not stored in DB)
                    time_diffs = calculate_time_differences(next_collection, last_collection)
                    item.update(time_diffs)
                    collections.append(item)
        else:
            # Single collection type
            if collection_type:
                collection_item["collection_type"] = collection_type
                # Add waste group
                waste_group = get_waste_group(collection_type)
                if waste_group:
                    collection_item["waste_group"] = waste_group
            if next_collection:
                collection_item["next_collection"] = next_collection
            if last_collection:
                collection_item["last_collection"] = last_collection
            # Calculate time differences for JSON output (not stored in DB)
            time_diffs = calculate_time_differences(next_collection, last_collection)
            collection_item.update(time_diffs)
            if collection_item:
                collections.append(collection_item)
    
    return collections


def get_bin_type_column_prefix(collection_type):
    """Get the column name prefix for a bin type.
    
    Args:
        collection_type: The raw collection type string from the website
    
    Returns:
        Column name prefix (e.g., "black_rubbish_140l") or None if not recognized
    """
    if not collection_type:
        return None
    
    collection_type_lower = collection_type.lower()
    
    # Black Rubbish (140L)
    if "black" in collection_type_lower and "rubbish" in collection_type_lower:
        return "black_rubbish_140l"
    
    # Blue Cardboard Bag
    if "blue" in collection_type_lower and ("cardboard" in collection_type_lower or "bag" in collection_type_lower):
        return "blue_cardboard_bag"
    
    # Food Waste Bin
    if "food" in collection_type_lower or "caddy" in collection_type_lower:
        return "black_food_waste"
    
    # Garden Waste (240L)
    if "garden" in collection_type_lower and "waste" in collection_type_lower:
        return "green_garden_bin"
    
    # Green Recycling Box
    if "green" in collection_type_lower and "recycling" in collection_type_lower:
        return "green_recycling_box"
    
    # If not recognized, return None (will be skipped)
    return None


def create_tables(conn):
    """Create simplified database table with columns for each bin type."""
    cursor = conn.cursor()
    
    # Create collections table with one row per address
    # Each bin type has its own last_collection and next_collection columns
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS collections (
            address TEXT PRIMARY KEY,
            black_rubbish_140l_last_collection TIMESTAMP WITH TIME ZONE,
            black_rubbish_140l_next_collection TIMESTAMP WITH TIME ZONE,
            blue_cardboard_bag_last_collection TIMESTAMP WITH TIME ZONE,
            blue_cardboard_bag_next_collection TIMESTAMP WITH TIME ZONE,
            black_food_waste_last_collection TIMESTAMP WITH TIME ZONE,
            black_food_waste_next_collection TIMESTAMP WITH TIME ZONE,
            green_garden_bin_last_collection TIMESTAMP WITH TIME ZONE,
            green_garden_bin_next_collection TIMESTAMP WITH TIME ZONE,
            green_recycling_box_last_collection TIMESTAMP WITH TIME ZONE,
            green_recycling_box_next_collection TIMESTAMP WITH TIME ZONE,
            site_last_checked TIMESTAMP WITH TIME ZONE NOT NULL
        )
    """)
    
    # Create index for site_last_checked
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_collections_site_last_checked ON collections(site_last_checked)
    """)
    
    conn.commit()
    cursor.close()




def store_collections(conn, address, postcode, collections_data):
    """Store collection data in the simplified database schema with columns for each bin type."""
    try:
        tz = ZoneInfo(TIMEZONE)
    except Exception:
        tz = ZoneInfo("Europe/London")
    site_last_checked = datetime.now(tz)
    
    cursor = conn.cursor()
    
    # Build update dictionary for all bin types
    update_fields = {}
    
    for collection in collections_data:
        collection_type_name = collection.get("collection_type")
        if not collection_type_name:
            continue
        
        # Get column prefix for this bin type
        column_prefix = get_bin_type_column_prefix(collection_type_name)
        if not column_prefix:
            # Skip unrecognized bin types
            continue
        
        # Parse datetime values (keep full datetime, not just date)
        next_collection_dt = None
        last_collection_dt = None
        
        next_collection_str = collection.get("next_collection")
        if next_collection_str and next_collection_str.lower() not in ["unknown", "n/a", ""]:
            next_dt = parse_collection_date(next_collection_str)
            if next_dt:
                next_collection_dt = next_dt
        
        last_collection_str = collection.get("last_collection")
        if last_collection_str and last_collection_str.lower() not in ["unknown", "n/a", ""]:
            last_dt = parse_collection_date(last_collection_str)
            if last_dt:
                last_collection_dt = last_dt
        
        # Store in update dictionary
        update_fields[f"{column_prefix}_last_collection"] = last_collection_dt
        update_fields[f"{column_prefix}_next_collection"] = next_collection_dt
    
    # Build SQL for INSERT ... ON CONFLICT UPDATE
    # Start with address and site_last_checked
    columns = ["address", "site_last_checked"]
    values = [address, site_last_checked]
    placeholders = ["%s", "%s"]
    update_parts = ["site_last_checked = EXCLUDED.site_last_checked"]
    
    # Add all bin type columns
    for column_prefix in ["black_rubbish_140l", "blue_cardboard_bag", "black_food_waste", 
                          "green_garden_bin", "green_recycling_box"]:
        last_col = f"{column_prefix}_last_collection"
        next_col = f"{column_prefix}_next_collection"
        
        columns.append(last_col)
        columns.append(next_col)
        
        # Use value from update_fields if available, otherwise None
        values.append(update_fields.get(last_col))
        values.append(update_fields.get(next_col))
        
        placeholders.append("%s")
        placeholders.append("%s")
        
        update_parts.append(f"{last_col} = EXCLUDED.{last_col}")
        update_parts.append(f"{next_col} = EXCLUDED.{next_col}")
    
    # Build and execute SQL
    sql = f"""
        INSERT INTO collections ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (address) DO UPDATE SET
            {', '.join(update_parts)}
    """
    
    cursor.execute(sql, values)
    
    conn.commit()
    cursor.close()
    print(f"✓ Stored collection data for {len([k for k in update_fields.keys() if k.endswith('_last_collection')])} bin types in database")


def main():
    """Main function to fetch and parse collection dates."""
    print(f"Starting bin collection scraper...", flush=True)
    print(f"  Postcode: {POSTCODE}", flush=True)
    print(f"  Address: {ADDRESS_TEXT}", flush=True)
    print(f"  Timezone: {TIMEZONE}", flush=True)
    print(f"  Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}", flush=True)
    print()
    
    with sync_playwright() as p:
        # Launch headless browser
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            # Navigate to the page
            page.goto(URL, wait_until="networkidle")
            
            # Find and fill the postcode input (not the header search)
            postcode_input = page.locator(
                "xpath=//label[contains(text(), 'Enter a postcode') or contains(text(), 'postcode')]/following::input[@type='text'] | "
                "//label[contains(text(), 'Enter a postcode') or contains(text(), 'postcode')]/../input[@type='text'] | "
                "//form//input[@type='text' and (contains(@name, 'postcode') or contains(@id, 'postcode'))] | "
                "//*[contains(@class, 'form') or contains(@id, 'form')]//input[@type='text'][not(contains(@class, 'search') or contains(@id, 'search'))]"
            ).first
            
            # Clear and enter postcode
            postcode_input.click()
            postcode_input.fill("")
            postcode_input.type(POSTCODE, delay=50)
            random_wait()
            
            # Verify postcode was entered
            entered_value = postcode_input.input_value()
            if entered_value != POSTCODE:
                postcode_input.fill("")
                postcode_input.type(POSTCODE, delay=50)
                random_wait()
            
            # Click the Find button
            find_button = page.locator(
                "xpath=//button[contains(., 'Find') or contains(text(), 'Find')] | //input[@type='submit' and contains(@value, 'Find')] | //button[@type='submit']"
            ).first
            
            find_button.click()
            
            # Wait for address dropdown to appear and populate
            print("Waiting for address dropdown to load...", flush=True)
            # First wait for the select element to be attached to the DOM
            # Try specific ID first, then fall back to generic select
            try:
                page.wait_for_selector("#PCSelectp1", state="attached", timeout=10000)
                select_locator = page.locator("#PCSelectp1")
            except Exception:
                page.wait_for_selector("select", state="attached", timeout=10000)
                select_locator = page.locator("select").first
            
            # Wait for the select to become visible (it might be hidden initially)
            try:
                select_locator.wait_for(state="visible", timeout=10000)
            except Exception:
                # If it doesn't become visible, try to interact with it anyway
                print("Warning: Select element found but not visible, attempting to interact anyway...", flush=True)
            
            random_wait()  # Wait for options to populate
            
            # Wait for options to populate
            def options_populated():
                options = select_locator.locator("option").all()
                return len(options) > 1
            
            # Wait up to 10 seconds for options
            for _ in range(20):
                if options_populated():
                    break
                random_wait()
            
            random_wait()
            
            # Select the address
            select = select_locator
            try:
                select.select_option(label=ADDRESS_TEXT)
            except Exception as e:
                # If normal selection fails (e.g., element is hidden), try JavaScript
                print(f"Warning: Normal select_option failed, trying JavaScript fallback...", flush=True)
                # Get all option labels and values using JavaScript (works even if hidden)
                options_data = page.evaluate("""
                    () => {
                        var select = document.querySelector('#PCSelectp1') || document.querySelector('select');
                        if (!select) return [];
                        var options = [];
                        for (var i = 0; i < select.options.length; i++) {
                            var opt = select.options[i];
                            if (opt.value && opt.text) {
                                options.push({value: opt.value, text: opt.text.trim()});
                            }
                        }
                        return options;
                    }
                """)
                
                if not options_data:
                    raise Exception("Could not retrieve options from dropdown")
                
                # Debug: print available options
                print(f"Available options in dropdown ({len(options_data)}):", flush=True)
                for opt in options_data[:5]:  # Show first 5
                    print(f"  - '{opt['text']}' (value: {opt['value']})", flush=True)
                if len(options_data) > 5:
                    print(f"  ... and {len(options_data) - 5} more", flush=True)
                
                # Try to find exact match
                option_value = None
                for opt in options_data:
                    if opt['text'] == ADDRESS_TEXT:
                        option_value = opt['value']
                        break
                
                # If no exact match, try partial match
                if not option_value:
                    for opt in options_data:
                        opt_text = opt['text'].lower()
                        addr_text = ADDRESS_TEXT.lower()
                        if addr_text in opt_text or opt_text in addr_text:
                            option_value = opt['value']
                            print(f"Found partial match: '{opt['text']}'", flush=True)
                            break
                
                if option_value:
                    # Use JavaScript to set the value
                    result = page.evaluate("""
                        (value) => {
                            var select = document.querySelector('#PCSelectp1') || document.querySelector('select');
                            if (select) {
                                select.value = value;
                                select.dispatchEvent(new Event('change', { bubbles: true }));
                                return select.value === value;
                            }
                            return false;
                        }
                    """, option_value)
                    if not result:
                        raise Exception(f"Failed to set select value to '{option_value}'")
                    print(f"Successfully selected address using JavaScript", flush=True)
                else:
                    # Print all available options for debugging
                    all_options = [opt['text'] for opt in options_data]
                    raise Exception(f"Could not find address '{ADDRESS_TEXT}' in dropdown. Available options: {all_options[:10]}")
            
            # Wait after address selection
            print("Waiting after address selection...", flush=True)
            random_wait()
            
            # Wait for and click "Find collection days" button
            print("Waiting for 'Find collection days' button...", flush=True)
            next_button = page.locator("#nextBtn")
            next_button.wait_for(state="visible", timeout=10000)
            next_button.click()
            
            # Wait for results table
            print("Waiting for collection dates to load...", flush=True)
            random_wait()
            
            page.wait_for_selector("table", state="visible", timeout=20000)
            
            # Get the page HTML and parse with BeautifulSoup
            html = page.content()
            collections = parse_collection_table(html)
            
            # Connect to database and store data
            print("Connecting to database...", flush=True)
            db_conn = psycopg2.connect(**DB_CONFIG)
            try:
                # Create tables if they don't exist
                create_tables(db_conn)
                print("✓ Database tables ready", flush=True)
                
                # Store collections in database
                store_collections(db_conn, ADDRESS_TEXT, POSTCODE, collections)
                
            finally:
                db_conn.close()
            
            # Output summary
            print(f"\n✓ Successfully processed {len(collections)} collection types for {ADDRESS_TEXT}")
            print(f"✓ Data stored in database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            
            # Output as JSON for verification (can be disabled for production)
            if os.getenv("DEBUG", "false").lower() == "true":
                output = {
                    "address": ADDRESS_TEXT,
                    "postcode": POSTCODE,
                    "timezone": TIMEZONE,
                    "collections": collections
                }
                print("\n" + json.dumps(output, indent=2))
            
        finally:
            browser.close()


if __name__ == "__main__":
    main()

