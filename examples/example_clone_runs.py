import mysql.connector
import time
import logging

# --- Configuration ---
# Configure logging to see the script's activity
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Remote (Source) Database Configuration ---
REMOTE_CONFIG = {
    'user': 'root',
    'password': 'secret',
    'host': '192.168.2.215',
    'database': '3t' # Assuming the database name is '3t'
}

# --- Local (Destination) Database Configuration ---
LOCAL_CONFIG = {
    'user': 'root',
    'password': 'secret',
    'host': 'localhost',
    'database': '3t' # Assuming the database name is '3t'
}

TABLE_TO_CLONE = 'runs'
CLONE_INTERVAL_SECONDS = 60

def clone_table():
    """
    Connects to the remote and local databases, and clones the specified table.
    """
    remote_conn = None
    local_conn = None

    try:
        # --- Connect to Remote Database ---
        logging.info("Connecting to remote database...")
        remote_conn = mysql.connector.connect(**REMOTE_CONFIG)
        remote_cursor = remote_conn.cursor(dictionary=True) # Use dictionary cursor for easy column mapping
        logging.info("Successfully connected to remote database.")

        # --- Connect to Local Database ---
        logging.info("Connecting to local database...")
        local_conn = mysql.connector.connect(**LOCAL_CONFIG)
        local_cursor = local_conn.cursor()
        logging.info("Successfully connected to local database.")

        # --- Fetch Data from Remote Table ---
        logging.info(f"Fetching all data from '{TABLE_TO_CLONE}' on remote server...")
        remote_cursor.execute(f"SELECT * FROM {TABLE_TO_CLONE}")
        all_rows = remote_cursor.fetchall()
        logging.info(f"Fetched {len(all_rows)} rows from remote table.")

        if not all_rows:
            logging.info("Remote table is empty. Clearing local table and finishing.")
            local_cursor.execute(f"DELETE FROM {TABLE_TO_CLONE}")
            local_conn.commit()
            return

        # --- Start Transaction on Local Database ---
        logging.info("Starting transaction on local database.")
        local_conn.start_transaction()

        # --- Clear the Local Table ---
        logging.info(f"Deleting all existing data from '{TABLE_TO_CLONE}' on local server...")
        local_cursor.execute(f"DELETE FROM {TABLE_TO_CLONE}")
        logging.info("Local table cleared.")

        # --- Insert Data into Local Table ---
        logging.info(f"Inserting {len(all_rows)} rows into local table...")
        
        # Prepare the insert statement dynamically based on columns
        columns = all_rows[0].keys()
        column_names = ', '.join(f"`{col}`" for col in columns)
        value_placeholders = ', '.join(['%s'] * len(columns))
        
        insert_query = f"INSERT INTO {TABLE_TO_CLONE} ({column_names}) VALUES ({value_placeholders})"
        
        # Prepare data for bulk insertion
        data_to_insert = [tuple(row.values()) for row in all_rows]

        # Execute the insertion
        local_cursor.executemany(insert_query, data_to_insert)
        
        # --- Commit the Transaction ---
        local_conn.commit()
        logging.info("Transaction committed. Data successfully cloned.")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        if local_conn and local_conn.is_connected():
            logging.warning("Rolling back local transaction due to error.")
            local_conn.rollback()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        # --- Close Connections ---
        if remote_conn and remote_conn.is_connected():
            remote_cursor.close()
            remote_conn.close()
            logging.info("Remote database connection closed.")
        if local_conn and local_conn.is_connected():
            local_cursor.close()
            local_conn.close()
            logging.info("Local database connection closed.")


def main():
    """
    Main function to run the cloning process in a loop.
    """
    logging.info("Starting MariaDB cloning script.")
    while True:
        logging.info("="*50)
        logging.info("Starting new cloning cycle.")
        clone_table()
        logging.info(f"Cloning cycle complete. Waiting for {CLONE_INTERVAL_SECONDS} seconds...")
        logging.info("="*50)
        time.sleep(CLONE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

