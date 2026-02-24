"""
Extract: Student Information System (SIS)
=========================================
Simulates extraction from a university SIS API/database.
In production, replace the Faker-based generator with real DB queries
or REST API calls to PeopleSoft / Banner / Workday Student.
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

from config.logging_config import logger
from config.settings import settings

fake = Faker()
random.seed(42)
Faker.seed(42)

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ── Constants ──────────────────────────────────────────────────────────────────
DEPARTMENTS = [
    ("CS001", "Computer Science",       "Engineering"),
    ("EE001", "Electrical Engineering", "Engineering"),
    ("BA001", "Business Administration","Business"),
    ("MA001", "Mathematics",            "Sciences"),
    ("PH001", "Physics",                "Sciences"),
    ("EN001", "English Literature",     "Arts & Humanities"),
    ("PS001", "Psychology",             "Social Sciences"),
    ("ME001", "Mechanical Engineering", "Engineering"),
]

PROGRAMS = {
    "CS001": ["B.Sc Computer Science", "M.Sc Data Science", "M.Sc AI"],
    "EE001": ["B.Sc Electrical Engineering", "M.Sc Embedded Systems"],
    "BA001": ["BBA", "MBA", "B.Sc Finance"],
    "MA001": ["B.Sc Mathematics", "M.Sc Statistics"],
    "PH001": ["B.Sc Physics", "M.Sc Astrophysics"],
    "EN001": ["B.A. English", "M.A. Literature"],
    "PS001": ["B.Sc Psychology", "M.Sc Clinical Psychology"],
    "ME001": ["B.Sc Mechanical Engineering", "M.Sc Robotics"],
}

STUDENT_TYPES = ["Full-Time", "Part-Time"]
GENDERS       = ["Male", "Female", "Non-Binary", "Prefer not to say"]
NATIONALITIES = ["Indian", "American", "Chinese", "British", "German",
                 "Nigerian", "Brazilian", "Canadian", "Australian", "Japanese"]


def generate_students(n: int = 2000) -> pd.DataFrame:
    """Generate synthetic student records."""
    logger.info(f"Generating {n} synthetic student records …")
    records = []
    for i in range(1, n + 1):
        dept_id, dept_name, college = random.choice(DEPARTMENTS)
        program   = random.choice(PROGRAMS[dept_id])
        major     = dept_name
        enroll_dt = fake.date_between(start_date="-4y", end_date="-1y")
        scholarship = random.random() < 0.20  # 20% on scholarship

        records.append({
            "student_id":        f"STU{i:06d}",
            "first_name":        fake.first_name(),
            "last_name":         fake.last_name(),
            "email":             fake.unique.email(),
            "date_of_birth":     fake.date_of_birth(minimum_age=17, maximum_age=35).isoformat(),
            "gender":            random.choice(GENDERS),
            "nationality":       random.choice(NATIONALITIES),
            "enrollment_date":   enroll_dt.isoformat(),
            "program":           program,
            "major":             major,
            "department_id":     dept_id,
            "student_type":      random.choice(STUDENT_TYPES),
            "scholarship":       str(scholarship),
            "scholarship_amount": round(random.uniform(5000, 50000), 2) if scholarship else 0.0,
        })

    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df)} student records")
    return df


def generate_enrollments(students_df: pd.DataFrame, n_courses: int = 80) -> pd.DataFrame:
    """Generate synthetic enrollment / grade records."""
    courses = _get_course_ids(n_courses)
    instructors = [f"INS{i:04d}" for i in range(1, 51)]

    semesters = [
        ("Fall 2022",   "2022-2023"),
        ("Spring 2023", "2022-2023"),
        ("Fall 2023",   "2023-2024"),
        ("Spring 2024", "2023-2024"),
        ("Fall 2024",   "2024-2025"),
        ("Spring 2025", "2024-2025"),
    ]

    GRADE_DIST = {
        "A+": (4.0, 0.05), "A":  (4.0, 0.15), "A-": (3.7, 0.10),
        "B+": (3.3, 0.12), "B":  (3.0, 0.15), "B-": (2.7, 0.10),
        "C+": (2.3, 0.08), "C":  (2.0, 0.08), "C-": (1.7, 0.06),
        "D":  (1.0, 0.05), "F":  (0.0, 0.04), "W":  (0.0, 0.02),
    }
    grade_letters = list(GRADE_DIST.keys())
    grade_weights = [GRADE_DIST[g][1] for g in grade_letters]

    logger.info("Generating enrollment / grade records …")
    records = []
    eid = 1
    for _, student in students_df.iterrows():
        n_enrollments = random.randint(3, 6)
        for sem, acad_year in random.sample(semesters, min(n_enrollments, len(semesters))):
            course_id  = random.choice(courses)
            instructor = random.choice(instructors)
            grade      = random.choices(grade_letters, weights=grade_weights)[0]
            gpoints    = GRADE_DIST[grade][0]
            attendance = round(random.gauss(82, 15), 1)
            attendance = max(0, min(100, attendance))

            records.append({
                "enrollment_id":          f"ENR{eid:08d}",
                "student_id":             student["student_id"],
                "course_id":              course_id,
                "instructor_id":          instructor,
                "semester":               sem,
                "academic_year":          acad_year,
                "enrollment_date":        fake.date_between(start_date="-3y", end_date="-1m").isoformat(),
                "grade":                  grade,
                "grade_points":           gpoints,
                "attendance_pct":         attendance,
                "assignments_completed":  random.randint(5, 12),
                "assignments_total":      12,
                "midterm_score":          round(random.gauss(72, 15), 1),
                "final_score":            round(random.gauss(74, 15), 1),
            })
            eid += 1

    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df)} enrollment records")
    return df


def _get_course_ids(n: int) -> list[str]:
    return [f"CRS{i:04d}" for i in range(1, n + 1)]


def extract_sis(output_dir: Path | None = None) -> dict[str, Path]:
    """
    Main extraction function.
    Returns dict of {entity: file_path} for downstream pipeline.
    """
    out = output_dir or RAW_DIR
    out.mkdir(parents=True, exist_ok=True)

    students_df    = generate_students(n=2000)
    enrollments_df = generate_enrollments(students_df)

    students_path    = out / "students.csv"
    enrollments_path = out / "enrollments.csv"

    students_df.to_csv(students_path,    index=False)
    enrollments_df.to_csv(enrollments_path, index=False)

    logger.info(f"SIS extract complete → {students_path}, {enrollments_path}")
    return {
        "students":    students_path,
        "enrollments": enrollments_path,
    }


if __name__ == "__main__":
    paths = extract_sis()
    for entity, path in paths.items():
        print(f"{entity:15s} → {path}")