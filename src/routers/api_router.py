from fastapi import APIRouter,UploadFile, Query,File
from fastapi.responses import JSONResponse
from sqlalchemy import text, asc, desc
from sqlalchemy.orm import aliased
from sqlalchemy import and_
import asyncio
from datetime import datetime
import logging
from typing import Optional
from db import session_maker, add_commit_refresh ,Event, Job, Progress
from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException


api_router = APIRouter()

file_queue = asyncio.Queue()

class QueuedFile:
    def __init__(self, filename: str, job_id:int, content: bytes):
        self.filename = filename
        self.content = content
        self.job_id= job_id

logging.basicConfig(filename='malformed_lines.log', level=logging.WARNING)


@api_router.post("/api/events/ingest")
async def process_file(file:UploadFile=File(...)):
    content = await file.read()
    db = session_maker()
    try:
        job = Job(file_name=file.filename, status="Processing")
        add_commit_refresh(db,job)

        q_file = QueuedFile(filename=file.filename, job_id=job.job_id, content=content)
        await file_queue.put(q_file)

        return {"message": f"File '{file.filename}' has been queued for processing.", "job_id": job.job_id}
    except Exception as e:
        raise e
    finally:
        db.close()

@api_router.get("/api/events/ingestion-status/{job_id}")
async def get_job_status(job_id: int):
    db = session_maker()
    try:
        job = db.query(Job).filter_by(job_id=job_id).first()
        if not job:
            return JSONResponse(status_code=404, content={"detail": "Job not found"})

        process_lines = db.query(Progress).filter_by(job_id=job_id).all()

        report = {
            "processed_lines": 0,
            "error_lines": 0,
            "errors": []
        }

        for line in process_lines:
            if line.line_status == "Completed":
                report["processed_lines"] += 1
            else:
                report["error_lines"] += 1
                report["errors"].append(line.error_message)

        return {"Status": job.status, **report}

    except Exception as e:
        # You can log the exception if needed
        raise e

    finally:
        db.close()


@api_router.get("/api/timeline/{root_event_id}")
async def get_event_timeline(root_event_id: str):
    db = session_maker()
    try:
        event=db.query(Event).filter_by(event_id=root_event_id).first()
        
        if not event:
            return JSONResponse(status_code=404,content="Record not found")
        # 1) Parent chain query
        parent_query = text("""
            WITH RECURSIVE parent_chain AS (
                SELECT e.*
                FROM events e
                WHERE e.event_id = :root_event_id

                UNION ALL

                SELECT p.*
                FROM events p
                JOIN parent_chain pc ON pc.parent_id = p.event_id
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    'event_id', event_id,
                    'event_name', event_name,
                    'start_date', start_date,
                    'end_date', end_date,
                    'duration_minutes', duration_minutes,
                    'description', description
                ) ORDER BY start_date
            ) AS parent_chain_json
            FROM parent_chain;
        """)
        parent_result = db.execute(parent_query, {"root_event_id": root_event_id}).scalar()

        # 2) Children hierarchy query
        children_query = text("""
            WITH RECURSIVE event_hierarchy AS (
                SELECT 
                    e.event_id,
                    e.event_name,
                    e.start_date,
                    e.end_date,
                    e.duration_minutes,
                    e.parent_id,
                    e.description,
                    0 AS level
                FROM events e
                WHERE e.event_id = :root_event_id

                UNION ALL

                SELECT 
                    c.event_id,
                    c.event_name,
                    c.start_date,
                    c.end_date,
                    c.duration_minutes,
                    c.parent_id,
                    c.description,
                    eh.level + 1
                FROM events c
                JOIN event_hierarchy eh ON c.parent_id = eh.event_id
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    'event_id', event_id,
                    'event_name', event_name,
                    'start_date', start_date,
                    'end_date', end_date,
                    'duration_minutes', duration_minutes,
                    'description', description,
                    'parent_id', parent_id
                )
            ) AS events_json
            FROM event_hierarchy;
        """)
        children_result = db.execute(children_query, {"root_event_id": root_event_id}).scalar()

        # ✅ Handle missing root event
        if not children_result:
            raise HTTPException(status_code=404, detail=f"Event {root_event_id} not found")

        return {
            "root_event_id": root_event_id,
            "event_name": event.event_name,
            "start_date": event.start_date,
            "end_date": event.end_date,
            "duration_minutes": event.duration_minutes,
            "description": event.description,
            "parents": parent_result if parent_result else [],
            "children": children_result
        }

    except SQLAlchemyError as db_err:
        logging.error(f"Database error while fetching timeline for {root_event_id}: {db_err}")
        raise HTTPException(status_code=500, detail="Database error occurred")

    except Exception as e:
        logging.error(f"Unexpected error while fetching timeline for {root_event_id}: {e}")
        raise HTTPException(status_code=500, detail="Unexpected server error")

    finally:
        db.close()

@api_router.get("/api/events/search")
def search_events(
    event_name: Optional[str] = Query(None, description="Partial event name search"),
    start_date_after: Optional[datetime] = Query(None, description="Filter events starting after this date"),
    end_date_before: Optional[datetime] = Query(None, description="Filter events ending before this date"),
    limit: int = Query(10, ge=1, le=100, description="Number of results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    sort_by: str = Query("start_date", description="Field to sort by: start_date, end_date, event_name"),
    sort_order: str = Query("asc", description="Sort order: asc or desc"),
):
    db = session_maker()

    try:
        query = db.query(Event)

        # 🔎 Partial string matching (case-insensitive)
        if event_name:
            query = query.filter(Event.event_name.ilike(f"%{event_name}%"))

        # 📅 Date filters
        if start_date_after:
            query = query.filter(Event.start_date >= start_date_after)

        if end_date_before:
            query = query.filter(Event.end_date <= end_date_before)

        # ↕️ Sorting
        sort_field_map = {
            "start_date": Event.start_date,
            "end_date": Event.end_date,
            "event_name": Event.event_name,
        }
        sort_field = sort_field_map.get(sort_by, Event.start_date)

        if sort_order.lower() == "desc":
            query = query.order_by(desc(sort_field))
        else:
            query = query.order_by(asc(sort_field))

        # Pagination
        events = query.offset(offset).limit(limit).all()

        # Response
        results = [
            {
                "event_id": e.event_id,
                "event_name": e.event_name,
                "start_date": e.start_date,
                "end_date": e.end_date,
                "duration_minutes": e.duration_minutes,
                "parent_id": e.parent_id,
                "description": e.description,
            }
            for e in events
        ]

        return {
            "total_results": query.count(),
            "limit": limit,
            "offset": offset,
            "events": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching events: {str(e)}")

    finally:
        db.close()

@api_router.get("/api/insights/overlapping-events")
def get_all_overlapping_events():
    db = session_maker()
    try:
        e1 = aliased(Event)
        e2 = aliased(Event)

        # Self-join to find overlapping event pairs
        overlaps = (
            db.query(e1, e2)
            .filter(e1.event_id < e2.event_id)  # prevent duplicate & self comparison
            .filter(
                and_(
                    e1.start_date < e2.end_date,
                    e1.end_date > e2.start_date
                )
            )
            .all()
        )

        result = []
        for ev1, ev2 in overlaps:
            # Calculate overlap duration in minutes
            overlap_start = max(ev1.start_date, ev2.start_date)
            overlap_end = min(ev1.end_date, ev2.end_date)
            overlap_duration = max(0, (overlap_end - overlap_start).total_seconds() // 60)

            result.append({
                "overlappingEventPairs": [
                    {
                        "event_id": ev1.event_id,
                        "event_name": ev1.event_name,
                        "start_date": ev1.start_date.isoformat() + "Z",
                        "end_date": ev1.end_date.isoformat() + "Z"
                    },
                    {
                        "event_id": ev2.event_id,
                        "event_name": ev2.event_name,
                        "start_date": ev2.start_date.isoformat() + "Z",
                        "end_date": ev2.end_date.isoformat() + "Z"
                    }
                ],
                "overlap_duration_minutes": int(overlap_duration)
            })

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching overlaps: {str(e)}")
    finally:
        db.close()
