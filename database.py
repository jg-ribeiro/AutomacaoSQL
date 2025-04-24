import os
import sqlite3

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
        * job_id: INTEGER PRIMARY KEY (FK -> jobs_he.job_id)
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

    # Commit and close
    conn.commit()
    conn.close()


if __name__ == '__main__':
    setup_database()
    print(f"Database initialized at: {DB_PATH}")

    # Connect to SQLite database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
            INSERT INTO jobs_he_new
            SELECT job_id, job_name, 'Y' as job_status, export_type, export_path, export_name, days_offset,
            CASE
                WHEN check_parameter = '0' THEN 'N'
                ELSE 'Y'
            END AS check_parameter,
            parameter_id, data_primary_key, sql_script FROM jobs_he
            WHERE job_status IN ('Y','N')
        """
    )

    cursor.execute(
        """
        DROP TABLE jobs_he;
        """
    )

    cursor.execute(
        """
        ALTER TABLE jobs_he_new RENAME TO jobs_he
        """
    )

    conn.commit()