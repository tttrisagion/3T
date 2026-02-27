"""
Purge Stale Providence Runs
"""

import os

from celery.utils.log import get_task_logger

from shared.celery_app import app
from shared.config import Config
from shared.database import get_db_connection
from shared.opentelemetry_config import get_tracer

logger = get_task_logger(__name__)
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


@app.task(name="worker.purge.purge_stale_runs")
def purge_stale_runs():
    """
    Finds and marks stale, unprofitable runs to be exited.
    """
    with tracer.start_as_current_span("purge_stale_runs_task") as span:
        db_cnx = None
        try:
            config = Config()
            purge_age = config.get("providence.purge_age_minutes", 180)

            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()

            query = """
                UPDATE runs
                SET exit_run = 1
                WHERE end_time IS NULL
                  AND TIMESTAMPDIFF(MINUTE, start_time, NOW()) > %s
                  AND position_direction = 0
                  AND exit_run = 0
                  AND height IS NULL;
            """
            cursor.execute(query, (purge_age,))
            db_cnx.commit()

            rows_affected = cursor.rowcount
            logger.info(f"Purged {rows_affected} stale runs.")
            span.set_attribute("purged_runs_count", rows_affected)

        except Exception as e:
            if db_cnx:
                db_cnx.rollback()
            logger.error(f"Failed to purge stale runs: {e}", exc_info=True)
            span.record_exception(e)
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()
