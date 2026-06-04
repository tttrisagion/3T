"""
Purge Stale Providence Runs
"""

import os

from celery.utils.log import get_task_logger

from shared.celery_app import app
from shared.database import get_db_connection
from shared.opentelemetry_config import get_tracer

logger = get_task_logger(__name__)
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


@app.task(name="worker.purge.purge_stale_runs")
def purge_stale_runs():
    """
    Surgically purges dead and orphaned runs to keep the runs table lightweight and fast.
    """
    with tracer.start_as_current_span("purge_stale_runs_task") as span:
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()

            query = """
                DELETE FROM runs
                WHERE (exit_run = 1 AND height IS NULL AND update_time < NOW() - INTERVAL 1 HOUR)
                   OR (end_time IS NOT NULL AND height IS NULL);
            """
            cursor.execute(query)
            db_cnx.commit()

            rows_affected = cursor.rowcount
            logger.info(f"Surgically purged {rows_affected} dead/orphaned runs.")
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
