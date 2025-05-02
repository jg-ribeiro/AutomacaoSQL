import os
import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash

def set_password(self, pw):
        self.password_hash = generate_password_hash(pw)

# Path to your SQLite database file
DB_PATH = os.path.join(os.path.dirname(__file__), 'jobs.db')


def setup_database(db_path: str = DB_PATH) -> None:
    """
    Create the SQLite database and required tables:
      - parameters
      - jobs_he
      - job_de

    Tables:
      parameters:
        * parameter_id: INTEGER PRIMARY KEY
        * parameter_name: TEXT (not null)
        * sql_script: TEXT (allows multilines)

      jobs_he:
        * job_id: INTEGER PRIMARY KEY
        * job_name: TEXT (not null)
        * job_status: INTEGER (not null)
        * export_type: TEXT (not null)
        * export_path: TEXT (not null)
        * export_name: TEXT (not null)
        * days_offset INTEGER
        * check_parameter: TEXT
        * parameter_id: INTEGER (foreign key -> parameters.parameter_id)
        * data_primary_key: TEXT (not a table PK)
        * sql_script: TEXT (allows multilines)

      jobs_de:
        * schedule_id: INTEGER PRIMARY KEY
        * job_id: INTEGER (FK -> jobs_he.job_id)
        * job_minute: TEXT
        * job_hour: TEXT
        * job_day: TEXT
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Enforce foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Create parameters table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS parameters (
            parameter_id INTEGER PRIMARY KEY,
            parameter_name TEXT NOT NULL,
            sql_script TEXT
        )
    """
    )

    # Create jobs_he table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_he_new (
            job_id INTEGER PRIMARY KEY,
            job_name TEXT NOT NULL,
            job_status TEXT NOT NULL CHECK (job_status IN ('Y','N')),
            export_type TEXT NOT NULL,
            export_path TEXT NOT NULL,
            export_name TEXT NOT NULL,
            days_offset INTEGER,
            check_parameter TEXT NOT NULL CHECK (job_status IN ('Y','N')),
            parameter_id INTEGER,
            data_primary_key TEXT,
            sql_script TEXT,
            FOREIGN KEY(parameter_id) REFERENCES parameters(parameter_id)
        )
    """
    )

    # Create job_de table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_de (
            schedule_id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            job_minute TEXT NOT NULL,
            job_hour TEXT NOT NULL,
            job_day TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs_he(job_id)
        )
    """
    )

    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            password_hash TEXT,
            role TEXT
        )
    """
    )

    # Create log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            log_id INTEGER PRIMARY KEY,
            timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            log_level TEXT,
            job_id INTEGER,
            log_text TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs_he(job_id)
        )
    """
    )

    # Create root user for first time
    pwd = generate_password_hash(input('Sua senha (bem forte) do root:'))
    cursor.execute(
         """INSERT INTO users (username, password_hash, role)
            VALUES (?, ?, ?)
         """,
         ('root', pwd, 'root')
    )

    # Commit and close
    conn.commit()
    conn.close()


if __name__ == '__main__':
    setup_database()