"""
Extract: Attendance System
===========================
Pulls daily attendance records from RFID / LMS-based attendance tracking.
Also serves as Kafka producer for real-time streaming events.
"""
from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

from config.logging_config import logger
from config.settings import settings

fake = Faker()
random.seed(77)

RAW_DIR = Path("data/raw")

STATUSES     = ["PRESENT", "ABSENT", "LATE", "EXCUSED"]
STATUS_PROBS = [0.80, 0.10, 0.07, 0.03]
SOURCE_SYS   = ["LMS", "RFID", "MANUAL"]


def generate_attendance(
    student_ids: list[str],
    course_ids:  list[str],
    start_date:  date = date(2024, 9, 1),
    end_date:    date = date(2025, 4, 30),
) -> pd.DataFrame:
    """
    Generate synthetic daily attendance records.
    Classes are assumed Mon-Wed-Fri for each student×course pair.
    """
    records = []
    current = start_date

    while current <= end_date:
        # Only weekdays (Mon=0 … Fri=4)
        if current.weekday() < 5:
            # Sample ~30% of student-course combos per day (not all courses meet daily)
            sample_size = max(1, int(len(student_ids) * len(course_ids) * 0.03))
            for _ in range(sample_size):
                student = random.choice(student_ids)
                course  = random.choice(course_ids)
                status  = random.choices(STATUSES, weights=STATUS_PROBS)[0]

                records.append({
                    "student_id":      student,
                    "course_id":       course,
                    "attendance_date": current.isoformat(),
                    "status":          status,
                    "source_system":   random.choice(SOURCE_SYS),
                    "minutes_late":    random.randint(1, 30) if status == "LATE" else 0,
                })
        current += timedelta(days=1)

    df = pd.DataFrame(records).drop_duplicates(["student_id", "course_id", "attendance_date"])
    logger.info(f"Generated {len(df)} attendance records")
    return df


def extract_attendance(
    student_ids: list[str],
    course_ids:  list[str],
    output_dir:  Path | None = None,
) -> dict[str, Path]:
    """Write attendance CSV to raw landing zone."""
    out = output_dir or RAW_DIR
    out.mkdir(parents=True, exist_ok=True)

    df   = generate_attendance(student_ids, course_ids)
    path = out / "attendance.csv"
    df.to_csv(path, index=False)

    logger.info(f"Attendance extract complete → {path} ({len(df):,} rows)")
    return {"attendance": path}


# ── Kafka Producer (streaming simulation) ─────────────────────────────────────
def produce_attendance_events(
    student_ids: list[str],
    course_ids:  list[str],
    n_events:    int = 100,
) -> None:
    """
    Produce real-time attendance events to Kafka.
    Call this from a separate process or cron to simulate streaming.
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.warning("kafka-python not installed; skipping Kafka producer")
        return

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    for _ in range(n_events):
        event = {
            "student_id":      random.choice(student_ids),
            "course_id":       random.choice(course_ids),
            "attendance_date": date.today().isoformat(),
            "status":          random.choices(STATUSES, weights=STATUS_PROBS)[0],
            "source_system":   "RFID",
            "timestamp":       fake.iso8601(),
        }
        producer.send(settings.kafka_attendance_topic, value=event)
        logger.debug(f"Produced: {event}")

    producer.flush()
    producer.close()
    logger.info(f"Produced {n_events} attendance events to Kafka")


if __name__ == "__main__":
    import pandas as pd
    students_df = pd.read_csv("data/raw/students.csv")
    courses_df  = pd.read_csv("data/raw/courses.csv")

    paths = extract_attendance(
        student_ids=students_df["student_id"].tolist(),
        course_ids=courses_df["course_id"].tolist(),
    )
    for entity, path in paths.items():
        print(f"{entity:15s} → {path}")