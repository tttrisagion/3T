#!/usr/bin/env python3
import time

import mysql.connector

# Database connection settings
docker_username = "root"
docker_password = "secret"
docker_host = "192.168.2.215"
docker_database_name = "3t"

# global connection
cnx = mysql.connector.connect(
    user=docker_username,
    password=docker_password,
    host=docker_host,
    database=docker_database_name,
)


def position_time_stop():
    # if run is an hour old and doesn't have position direction, exit job
    cursor = cnx.cursor()

    count_query = """
        SELECT COUNT(id)
        FROM runs
        WHERE end_time IS NULL
          AND TIMESTAMPDIFF(MINUTE, start_time, NOW()) / 60 > 1
          AND position_direction = 0
          AND exit_run = 0;
    """
    cursor.execute(count_query)
    # fetchone() returns a tuple, e.g., (5,), so we access the first element
    items_to_purge = cursor.fetchone()[0]

    print(f"{time.time()} Found {items_to_purge} item(s) to be purged.")

    # --- Step 2: Run the original update command if there's anything to update ---
    if items_to_purge > 0:
        print("Purging items now...")
        query = """
UPDATE runs r1
JOIN (
  SELECT id
  FROM runs
  WHERE end_time IS NULL
    AND TIMESTAMPDIFF(MINUTE, start_time, NOW()) / 60 > 1
    AND position_direction = 0
    AND exit_run = 0
) AS r2 ON r1.id = r2.id
SET r1.exit_run = 1;
            """
        cursor.execute(query)
        cnx.commit()
        print("Done")
    else:
        print("No items to purge")


position_time_stop()
