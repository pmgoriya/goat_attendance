import psycopg2
import pytz
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

load_dotenv()

def get_db_connection():
    try:
        db_host = os.environ['DB_HOST']
        db_name = os.environ['DB_NAME']
        db_user = os.environ['DB_USER']
        db_password = os.environ['DB_PASSWORD']

        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise e

def process_absent_tags(conn, hours):
    # (The rest of the process_absent_tags function remains unchanged)
    print("Lambda function started.")

    # Create a cursor object
    cur = conn.cursor()

    # Get the timestamp for the specified hours ago
    ist_timezone = pytz.timezone('Asia/Kolkata')
    hours_ago = datetime.now(ist_timezone) - timedelta(hours=hours)
    current_time = datetime.now(ist_timezone)

    diff = relativedelta(current_time, hours_ago)
    hours_ago_str = hours_ago.strftime('%Y-%m-%d %H:%M:%S IST')

    print(f"Checking for absent tags from {diff.hours} hours, {diff.minutes} minutes, and {diff.seconds} seconds ago, which is {hours_ago_str}")

    # Calculate the threshold value for absent_count
    threshold = 6 * hours

    # Perform the INSERT operation
    cur.execute("""
        WITH tag_ids AS (
            SELECT tag_id
            FROM tag_details
            WHERE status = true
        ),
        tag_entries AS (
            SELECT tag_id, COUNT(*) AS entry_count
            FROM goats_vital
            WHERE tag_id IN (SELECT tag_id FROM tag_ids) AND created_at >= %s AND created_at < %s
            GROUP BY tag_id
        ),
        filtered_tags AS (
            SELECT tag_id, entry_count, %s - entry_count AS absent_count
            FROM tag_entries
            WHERE entry_count < %s
        )
        INSERT INTO goats_attendance (tag_id, red_flag_count)
        SELECT tag_id, absent_count
        FROM filtered_tags
        ON CONFLICT (tag_id) DO NOTHING;
    """, (hours_ago_str, datetime.now().strftime('%Y%m%d%H%M%S'), threshold, threshold))

    # Perform the UPDATE operation using the same CTE
    cur.execute("""
        WITH tag_ids AS (
            SELECT tag_id
            FROM tag_details
            WHERE status = true
        ),
        tag_entries AS (
            SELECT tag_id, COUNT(*) AS entry_count
            FROM goats_vital
            WHERE tag_id IN (SELECT tag_id FROM tag_ids) AND created_at >= %s AND created_at < %s
            GROUP BY tag_id
        ),
        filtered_tags AS (
            SELECT tag_id, entry_count, %s - entry_count AS absent_count
            FROM tag_entries
            WHERE entry_count < %s
        )
        UPDATE goats_attendance ga
        SET red_flag_count = ga.red_flag_count + ft.absent_count
        FROM filtered_tags ft
        WHERE ga.tag_id = ft.tag_id;
    """, (hours_ago_str, datetime.now().strftime('%Y%m%d%H%M%S'), threshold, threshold))

    # Construct the warning message
    cur.execute("""
        WITH tag_ids AS (
            SELECT tag_id
            FROM tag_details
            WHERE status = true
        ),
        tag_entries AS (
            SELECT tag_id, COUNT(*) AS entry_count
            FROM goats_vital
            WHERE tag_id IN (SELECT tag_id FROM tag_ids) AND created_at >= %s AND created_at < %s
            GROUP BY tag_id
        ),
        filtered_tags AS (
            SELECT tag_id, entry_count, %s - entry_count AS absent_count
            FROM tag_entries
            WHERE entry_count < %s
        ),
        warning_info AS (
            SELECT ft.tag_id, ft.absent_count, hgtj.goat_id, hgtj.farmer_id, hgtj.hub_id
            FROM filtered_tags ft
            JOIN hub_goat_tag_junction hgtj ON ft.tag_id = hgtj.tag_id
        )
        SELECT * FROM warning_info;
    """, (hours_ago_str, datetime.now().strftime('%Y%m%d%H%M%S'), threshold, threshold))

    # Fetch all the rows for the warning message
    warning_info = cur.fetchall()

    # Commit the changes
    conn.commit()
    print("Database changes committed.")

    # Close the cursor
    cur.close()

    # Generate the warning message
    if warning_info:
        warning_message = f"The following tags have been missing for the specified number of times in the last {hours} hours:\n"
        for info in warning_info:
            tag_id, absent_count, goat_id, farmer_id, hub_id = info
            warning_message += f"Tag ID: {tag_id}, Absent Count: {absent_count}, Goat ID: {goat_id}, Farmer ID: {farmer_id}, Hub ID: {hub_id}\n"
    else:
        warning_message = f"No absent tags found in the last {hours} hours."

    print("Lambda function completed.")
    return warning_message


def lambda_handler(event, context):
    try:
        conn = get_db_connection()
        warning_message = process_absent_tags(conn, 2)  # Change the hours value as needed
        conn.close()

        return {
            'statusCode': 200,
            'body': warning_message
        }
    except Exception as e:
        print(f"Error processing the request: {e}")
        return {
            'statusCode': 500,
            'body': str(e)
        }

def main():
    conn = get_db_connection()
    warning_message = process_absent_tags(conn, 2)  # Change the hours value as needed
    conn.close()

    print(warning_message)


if __name__ == "__main__":
    main()
