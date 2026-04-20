#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import mysql.connector

# Add project root to sys.path to access shared modules
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from shared.config import config


def cleanup(dry_run=False, host_override=None):
    active_symbols = config.get("reconciliation_engine.symbols", [])
    if not active_symbols:
        print(
            "Error: No active symbols found in reconciliation_engine.symbols in config.yml"
        )
        return

    print(f"Active symbols from config: {active_symbols}")

    db_host = host_override or config.get("database.host")
    print(f"Connecting to database at {db_host}...")

    conn = mysql.connector.connect(
        host=db_host,
        user=config.get("database.user"),
        password=config.get_secret("database.password"),
        database=config.get("database.database"),
    )
    cursor = conn.cursor()

    try:
        format_strings = ",".join(["%s"] * len(active_symbols))

        if dry_run:
            print("\n--- DRY RUN MODE: No changes will be committed ---")

            # 1. Count runs to be exited
            query_count_runs = f"SELECT COUNT(*) FROM runs WHERE symbol NOT IN ({format_strings}) AND exit_run = 0 AND end_time IS NULL"
            cursor.execute(query_count_runs, tuple(active_symbols))
            runs_to_exit = cursor.fetchone()[0]
            print(f"Runs that would be set to exited: {runs_to_exit}")

            # 2. Count products that would be kept
            query_keep_products = (
                f"SELECT symbol FROM products WHERE symbol IN ({format_strings})"
            )
            cursor.execute(query_keep_products, tuple(active_symbols))
            kept_products = [row[0] for row in cursor.fetchall()]
            print(f"Symbols that would be KEPT in products table: {kept_products}")

            # 3. Count products to be deleted
            query_count_products = (
                f"SELECT symbol FROM products WHERE symbol NOT IN ({format_strings})"
            )
            cursor.execute(query_count_products, tuple(active_symbols))
            products_to_delete = [row[0] for row in cursor.fetchall()]
            print(
                f"Symbols that would be DELETED from products table: {products_to_delete}"
            )

            # 4. Count positions to be deleted
            query_count_positions = f"""
                SELECT COUNT(*) FROM positions 
                WHERE product_id IN (
                    SELECT id FROM products WHERE symbol NOT IN ({format_strings})
                )
            """
            cursor.execute(query_count_positions, tuple(active_symbols))
            positions_to_delete = cursor.fetchone()[0]
            print(f"Position records that would be deleted: {positions_to_delete}")

            # 5. Count instruments to be deleted
            query_count_instruments = (
                """
                SELECT name FROM instruments 
                WHERE id NOT IN (
                    SELECT DISTINCT instrument_id FROM products 
                    WHERE symbol IN (%s)
                )
            """
                % format_strings
            )
            cursor.execute(query_count_instruments, tuple(active_symbols))
            instruments_to_delete = [row[0] for row in cursor.fetchall()]
            print(f"Instruments that would be deleted: {instruments_to_delete}")

            # 6. Count trading ranges to be deleted
            query_count_ranges = f"SELECT symbol FROM trading_range WHERE symbol NOT IN ({format_strings})"
            cursor.execute(query_count_ranges, tuple(active_symbols))
            ranges_to_delete = [row[0] for row in cursor.fetchall()]
            print(f"Trading ranges that would be deleted: {ranges_to_delete}")

            print("\nDry run completed. No changes made.")

        else:
            # 1. Mark runs for non-matching symbols as exited
            print("Marking inactive runs as exited...")
            query_marks_runs = f"""
                UPDATE runs 
                SET exit_run = 1, end_time = NOW() 
                WHERE symbol NOT IN ({format_strings}) 
                AND exit_run = 0 AND end_time IS NULL
            """
            cursor.execute(query_marks_runs, tuple(active_symbols))
            print(f"Updated {cursor.rowcount} runs.")

            # 2. Delete positions for non-matching products
            print("Deleting positions for inactive products...")
            query_delete_positions = f"""
                DELETE FROM positions 
                WHERE product_id IN (
                    SELECT id FROM products WHERE symbol NOT IN ({format_strings})
                )
            """
            cursor.execute(query_delete_positions, tuple(active_symbols))
            print(f"Deleted {cursor.rowcount} position records.")

            # 3. Delete from products table
            print("Deleting inactive products...")
            query_delete_products = (
                f"DELETE FROM products WHERE symbol NOT IN ({format_strings})"
            )
            cursor.execute(query_delete_products, tuple(active_symbols))
            print(f"Deleted {cursor.rowcount} product records.")

            # 4. Delete from instruments no longer associated with any product
            print("Cleaning up instruments...")
            query_delete_instruments = """
                DELETE FROM instruments 
                WHERE id NOT IN (SELECT DISTINCT instrument_id FROM products)
            """
            cursor.execute(query_delete_instruments)
            print(f"Deleted {cursor.rowcount} unused instrument records.")

            # 5. Clean up trading_range table
            print("Cleaning up trading ranges...")
            query_delete_ranges = (
                f"DELETE FROM trading_range WHERE symbol NOT IN ({format_strings})"
            )
            cursor.execute(query_delete_ranges, tuple(active_symbols))
            print(f"Deleted {cursor.rowcount} trading range records.")

            conn.commit()
            print("Cleanup completed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup inactive symbols and runs.")
    parser.add_argument(
        "--dryrun",
        "--dry-run",
        action="store_true",
        help="Run in dry run mode (SELECT only)",
    )
    parser.add_argument("--host", help="Database host (overrides config)")
    args = parser.parse_args()

    cleanup(dry_run=args.dryrun, host_override=args.host)
