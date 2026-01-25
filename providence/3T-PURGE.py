#!/usr/bin/env python3
import time

import mysql.connector

# Database connection settings
docker_username = "root"
docker_password = "secret"
docker_host = "192.168.2.157"
docker_database_name = "3t"

# global connection
cnx = mysql.connector.connect(
    user=docker_username,
    password=docker_password,
    host=docker_host,
    database=docker_database_name,
)


def position_time_stop():
    cursor = cnx.cursor()

    # --- Step 1: Count items based on PnL and Time threshold ---
    # Logic: Active runs, older than 180 mins, PnL < 0.2
    count_query = """
        SELECT COUNT(id)
        FROM runs
        WHERE end_time IS NULL
          AND TIMESTAMPDIFF(MINUTE, start_time, NOW()) / 180 > 1
          AND position_direction = 0
          AND exit_run = 0
          AND height IS NULL;
    """
    cursor.execute(count_query)
    # fetchone() returns a tuple, e.g., (5,), so we access the first element
    items_to_purge = cursor.fetchone()[0]

    print(
        f"{time.time()} Found {items_to_purge} item(s) to be purged based on PnL/Time."
    )

    # --- Step 2: Run the update command if there's anything to update ---
    if items_to_purge > 0:
        print("Purging items now...")

        # Using a JOIN to ensure we target the exact IDs identified in the logic above
        query = """
        UPDATE runs r1
        JOIN (
          SELECT id
          FROM runs
          WHERE end_time IS NULL
            AND TIMESTAMPDIFF(MINUTE, start_time, NOW()) / 180 > 1
            AND position_direction = 0
            AND exit_run = 0
            AND height IS NULL
        ) AS r2 ON r1.id = r2.id
        SET r1.exit_run = 1;
        """

        cursor.execute(query)
        cnx.commit()
        print("Done")
    else:
        print("No items to purge")

    cursor.close()


# Execute
position_time_stop()
