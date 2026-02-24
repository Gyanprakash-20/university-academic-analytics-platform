"""
Extract: Learning Management System (LMS)
==========================================
Simulates extraction from Moodle/Canvas-style LMS REST API.
Returns course catalog + assignment submission data.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from config.logging_config import logger

fake = Faker()
random.seed(99)
Faker.seed(99)

RAW_DIR = Path("data/raw")

COURSE_LEVELS      = ["Undergraduate", "Graduate"]
COURSE_CATEGORIES  = ["Core", "Elective", "Lab", "Seminar", "Thesis"]
DEPT_COURSES = {
    "CS001": [
        ("Introduction to Programming",     "CS101", 3, "Undergraduate", "Core"),
        ("Data Structures & Algorithms",    "CS201", 3, "Undergraduate", "Core"),
        ("Database Systems",                "CS301", 3, "Undergraduate", "Core"),
        ("Machine Learning",                "CS401", 3, "Graduate",      "Core"),
        ("Computer Networks",               "CS351", 3, "Undergraduate", "Elective"),
        ("Software Engineering",            "CS360", 3, "Undergraduate", "Core"),
        ("Cloud Computing",                 "CS450", 3, "Graduate",      "Elective"),
        ("Computer Vision",                 "CS490", 3, "Graduate",      "Elective"),
    ],
    "BA001": [
        ("Principles of Management",        "BA101", 3, "Undergraduate", "Core"),
        ("Financial Accounting",            "BA201", 3, "Undergraduate", "Core"),
        ("Business Analytics",              "BA301", 3, "Undergraduate", "Core"),
        ("Strategic Management",            "BA401", 3, "Graduate",      "Core"),
        ("Marketing Management",            "BA250", 3, "Undergraduate", "Elective"),
        ("Operations Management",           "BA310", 3, "Undergraduate", "Elective"),
    ],
    "MA001": [
        ("Calculus I",                      "MA101", 4, "Undergraduate", "Core"),
        ("Linear Algebra",                  "MA201", 3, "Undergraduate", "Core"),
        ("Probability & Statistics",        "MA301", 3, "Undergraduate", "Core"),
        ("Real Analysis",                   "MA401", 3, "Graduate",      "Core"),
        ("Numerical Methods",               "MA350", 3, "Undergraduate", "Elective"),
    ],
    "PH001": [
        ("Classical Mechanics",             "PH101", 4, "Undergraduate", "Core"),
        ("Electromagnetism",               "PH201", 4, "Undergraduate", "Core"),
        ("Quantum Mechanics",               "PH301", 3, "Undergraduate", "Core"),
        ("Astrophysics",                    "PH401", 3, "Graduate",      "Elective"),
    ],
    "EE001": [
        ("Circuit Analysis",               "EE101", 4, "Undergraduate", "Core"),
        ("Digital Electronics",            "EE201", 3, "Undergraduate", "Core"),
        ("Signals & Systems",              "EE301", 3, "Undergraduate", "Core"),
        ("Embedded Systems",               "EE401", 3, "Graduate",      "Core"),
    ],
    "EN001": [
        ("Academic Writing",               "EN101", 3, "Undergraduate", "Core"),
        ("World Literature",               "EN201", 3, "Undergraduate", "Elective"),
        ("Creative Writing",               "EN301", 3, "Undergraduate", "Elective"),
        ("Literary Theory",                "EN401", 3, "Graduate",      "Core"),
    ],
    "PS001": [
        ("Introduction to Psychology",     "PS101", 3, "Undergraduate", "Core"),
        ("Cognitive Psychology",           "PS201", 3, "Undergraduate", "Core"),
        ("Research Methods",               "PS301", 3, "Undergraduate", "Core"),
        ("Clinical Psychology",            "PS401", 3, "Graduate",      "Core"),
    ],
    "ME001": [
        ("Engineering Drawing",            "ME101", 3, "Undergraduate", "Core"),
        ("Thermodynamics",                 "ME201", 3, "Undergraduate", "Core"),
        ("Fluid Mechanics",                "ME301", 3, "Undergraduate", "Core"),
        ("Robotics",                       "ME401", 3, "Graduate",      "Elective"),
    ],
}


def generate_courses() -> pd.DataFrame:
    """Build course catalog from LMS metadata."""
    records = []
    cid = 1
    for dept_id, courses in DEPT_COURSES.items():
        for name, code, credits, level, category in courses:
            records.append({
                "course_id":    f"CRS{cid:04d}",
                "course_code":  code,
                "course_name":  name,
                "credit_hours": credits,
                "level":        level,
                "category":     category,
                "department_id": dept_id,
                "is_active":    True,
            })
            cid += 1
    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df)} course records from LMS")
    return df


def generate_instructors() -> pd.DataFrame:
    """Generate instructor records."""
    ranks = ["Professor", "Associate Professor", "Assistant Professor", "Lecturer"]
    tenure = {"Professor": "Tenured", "Associate Professor": "Tenured",
               "Assistant Professor": "Tenure-Track", "Lecturer": "Non-Tenure"}
    records = []
    for i in range(1, 51):
        dept_id = random.choice([d[0] for d in [
            ("CS001",), ("EE001",), ("BA001",), ("MA001",),
            ("PH001",), ("EN001",), ("PS001",), ("ME001",),
        ]])
        rank = random.choice(ranks)
        records.append({
            "instructor_id":  f"INS{i:04d}",
            "first_name":     fake.first_name(),
            "last_name":      fake.last_name(),
            "email":          fake.unique.company_email(),
            "rank":           rank,
            "tenure_status":  tenure[rank],
            "department_id":  dept_id,
            "hire_date":      fake.date_between(start_date="-20y", end_date="-1y").isoformat(),
            "is_active":      True,
        })
    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df)} instructor records from LMS")
    return df


def extract_lms(output_dir: Path | None = None) -> dict[str, Path]:
    """Main LMS extraction function."""
    out = output_dir or RAW_DIR
    out.mkdir(parents=True, exist_ok=True)

    courses_df     = generate_courses()
    instructors_df = generate_instructors()

    courses_path     = out / "courses.csv"
    instructors_path = out / "instructors.csv"

    courses_df.to_csv(courses_path,       index=False)
    instructors_df.to_csv(instructors_path, index=False)

    logger.info(f"LMS extract complete → {courses_path}, {instructors_path}")
    return {
        "courses":     courses_path,
        "instructors": instructors_path,
    }


if __name__ == "__main__":
    paths = extract_lms()
    for entity, path in paths.items():
        print(f"{entity:15s} → {path}")