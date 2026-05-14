from __future__ import annotations

import sqlite3
from pathlib import Path


DEFAULT_DB_PATH = Path(__file__).with_name("sqlite_lab.db")

SCHEMA_SQL = """
DROP TABLE IF EXISTS enrollments;
DROP TABLE IF EXISTS courses;
DROP TABLE IF EXISTS students;

CREATE TABLE students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    cohort TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    score REAL NOT NULL CHECK (score >= 0 AND score <= 100),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE courses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    credits INTEGER NOT NULL CHECK (credits > 0)
);

CREATE TABLE enrollments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
    course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('active', 'completed', 'dropped')),
    grade REAL CHECK (grade IS NULL OR (grade >= 0 AND grade <= 100)),
    UNIQUE (student_id, course_id)
);
"""

SEED_SQL = """
INSERT INTO students (name, cohort, email, score) VALUES
    ('Ada Lovelace', 'A1', 'ada@example.edu', 96.5),
    ('Grace Hopper', 'A1', 'grace@example.edu', 91.0),
    ('Katherine Johnson', 'B2', 'katherine@example.edu', 98.0),
    ('Dorothy Vaughan', 'B2', 'dorothy@example.edu', 88.5),
    ('Mary Jackson', 'C3', 'mary@example.edu', 93.0);

INSERT INTO courses (code, title, credits) VALUES
    ('MCP101', 'Model Context Protocol Basics', 3),
    ('SQL201', 'Practical SQLite', 4),
    ('AI305', 'Applied Tool Integration', 3);

INSERT INTO enrollments (student_id, course_id, status, grade) VALUES
    (1, 1, 'completed', 97.0),
    (1, 2, 'active', NULL),
    (2, 1, 'completed', 90.0),
    (3, 2, 'completed', 99.0),
    (4, 3, 'active', NULL),
    (5, 1, 'completed', 94.0);
"""


def create_database(db_path: str | Path = DEFAULT_DB_PATH) -> Path:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA_SQL)
        conn.executescript(SEED_SQL)
        conn.commit()
    return path


if __name__ == "__main__":
    created_path = create_database()
    print(created_path)
