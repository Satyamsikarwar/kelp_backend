from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey, Text, Enum
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from dotenv import load_dotenv
import os
import enum

load_dotenv()
DB_PASSWORD=os.getenv("DB_PASSWORD")
print(DB_PASSWORD)
DATABASE_URL =  f"postgresql://postgres:{DB_PASSWORD}@localhost:5432/bcg_test"
engine = create_engine(DATABASE_URL) 
Base = declarative_base()

class Event(Base):
    __tablename__ = "events"

    event_id = Column(String, primary_key=True, index=True)
    event_name = Column(String, nullable=False, index=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    duration_minutes = Column(Integer, nullable=False)
    parent_id = Column(String, ForeignKey("events.event_id"), nullable=True)
    description = Column(Text, nullable=True)

    # Self-referencing relationship
    parent = relationship("Event", remote_side=[event_id], backref="children")


class JobStatus(enum.Enum):
    Processing = "Processing"
    Completed = "Completed"
    Failed = "Failed"

class Job(Base):
    __tablename__ = "jobs"

    job_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    file_name = Column(String, nullable=False)
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.Processing)

    # Relationship with Progress table
    progress_entries = relationship(
        "Progress",
        back_populates="job",
        cascade="all, delete-orphan"
    )


class Progress(Base):
    __tablename__ = "progress"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    job_id = Column(Integer, ForeignKey("jobs.job_id", ondelete="CASCADE"), nullable=False, index=True)
    line_status = Column(String, nullable=False)  # e.g., "Processed" or "Failed"
    error_message = Column(Text, nullable=True)

    # Relationship back to Job
    job = relationship("Job", back_populates="progress_entries")

Base.metadata.create_all(engine)
session_maker = sessionmaker(bind=engine)
def get_db():
    db = session_maker()
    try:
        yield db
    finally:
        db.close()


def add_commit_refresh(db,content):
    db.add(content)
    db.commit()
    db.refresh(content)
