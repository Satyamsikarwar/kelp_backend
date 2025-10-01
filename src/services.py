import uuid
from datetime import datetime
from routers.api_router import file_queue
from io import StringIO
from db import session_maker, Event, Job, Progress
from sqlalchemy import text
import logging
from sqlalchemy.exc import SQLAlchemyError

def is_valid_uuid(value):
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False

def is_valid_iso_date(value):
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))  # Handles 'Z' as UTC
        return True
    except ValueError:
        return False

def process_line(line):
    parts = line.strip().split('|')
    
    if len(parts) != 6:
        raise ValueError("Incorrect number of fields")

    event_id, event_name, start_date, end_date, parent_id, description = parts

    if not is_valid_uuid(event_id):
        raise ValueError("Invalid EVENT_ID (UUID expected)")

    if not is_valid_iso_date(start_date):
        raise ValueError("Invalid START_DATE_ISO")

    if not is_valid_iso_date(end_date):
        raise ValueError("Invalid END_DATE_ISO")

    if parent_id != "NULL" and not is_valid_uuid(parent_id):
        raise ValueError("Invalid PARENT_ID (UUID or 'NULL' expected)")

    start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))

    duration = (end_dt - start_dt).total_seconds() // 60
    if duration < 0:
        raise ValueError("END_DATE_ISO is before START_DATE_ISO")

    return {
        "event_id": event_id,
        "event_name": event_name,
        "start_date": start_dt,
        "end_date": end_dt,
        "parent_id": None if parent_id == "NULL" else parent_id,
        "description": description,
        "duration_minutes": int(duration)
    }

async def process_files():
    print("Background file processor started")
    while True:
        queued_file = await file_queue.get()
        filename = queued_file.filename
        content = queued_file.content.decode("utf-8", errors="ignore")
        job_id = queued_file.job_id

        print(f"Processing file: {filename}")
        file_like = StringIO(content)
        db = session_maker()

        try:
            job_update = db.query(Job).filter_by(job_id=job_id).first()

            for line_number, line in enumerate(file_like, 1):
                line_status = "Completed"
                error_message = None
                try:
                    event_data = process_line(line)
                    print("event data", event_data)

                    event = Event(
                        event_id=event_data["event_id"],
                        event_name=event_data["event_name"],
                        start_date=event_data["start_date"],
                        end_date=event_data["end_date"],
                        duration_minutes=event_data["duration_minutes"],
                        parent_id=event_data["parent_id"],
                        description=event_data["description"],
                    )

                    # Safe insert with ON CONFLICT DO NOTHING (Postgres)
                    db.execute(
                    text(
                        """
                        INSERT INTO events (event_id, event_name, start_date, end_date, duration_minutes, parent_id, description)
                        VALUES (:event_id, :event_name, :start_date, :end_date, :duration_minutes, :parent_id, :description)
                        ON CONFLICT (event_id) DO NOTHING
                        """
                    ),
                    event_data,
                    )

                except Exception as e:
                    db.rollback()  # rollback for this line before continuing
                    logging.warning(
                        f"[{filename}] Line {line_number}: {str(e)} | Content: {line.strip()}"
                    )
                    line_status = "Failed"
                    error_message = str(e)

                finally:
                    # Insert a progress row for this line
                    try:
                        progress_entry = Progress(
                            job_id=job_id,
                            line_status=line_status,
                            error_message=error_message,
                        )
                        db.add(progress_entry)
                        db.commit()
                    except Exception as progress_error:
                        db.rollback()
                        logging.error(
                            f"Failed to insert progress for line {line_number} in file {filename}: {progress_error}"
                        )

            # Commit all inserts after file processed
            db.commit()

            if job_update:
                job_update.status = "Completed"
                db.commit()
                print(f"Job updated: {job_update.job_id} -> Completed")

            print(f"Finished processing {filename}")

        except SQLAlchemyError as e:
            db.rollback()
            if job_update:
                try:
                    job_update.status = "Failed"
                    db.commit()
                    print(f"Job updated: {job_update.job_id} -> Failed")
                except Exception as inner_e:
                    db.rollback()
                    logging.error(f"Failed to update job status: {inner_e}")

            logging.error(f"Database error while processing file {filename}: {e}")

        except Exception as e:
            db.rollback()
            if job_update:
                try:
                    job_update.status = "Failed"
                    db.commit()
                    print(f"Job updated: {job_update.job_id} -> Failed")
                except Exception as inner_e:
                    db.rollback()
                    logging.error(f"Failed to update job status: {inner_e}")

            logging.error(f"Unexpected error while processing file {filename}: {e}")

        finally:
            db.close()
            file_queue.task_done()