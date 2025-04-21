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
        * export_type: TEXT (not null)
        * export_path: TEXT (not null)
        * export_name: TEXT (not null)
        * check_parameters: TEXT
        * parameter_id: INTEGER (foreign key -> parameters.parameter_id)
        * data_primary_key: TEXT (not a table PK)
        * sql_script: TEXT (allows multilines)

      job_de:
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
        CREATE TABLE IF NOT EXISTS jobs_he (
            job_id INTEGER PRIMARY KEY,
            job_name TEXT NOT NULL,
            export_type TEXT NOT NULL,
            export_path TEXT NOT NULL,
            export_name TEXT NOT NULL,
            check_parameters TEXT,
            parameter_id INTEGER,
            data_primary_key TEXT NOT NULL,
            sql_script TEXT,
            FOREIGN KEY(parameter_id) REFERENCES parameters(parameter_id)
        )
    """
    )

    # Create job_de table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_de (
            job_id INTEGER PRIMARY KEY,
            job_minute TEXT,
            job_hour TEXT,
            job_day TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs_he(job_id)
        )
    """
    )

    # Commit and close
    conn.commit()
    conn.close()


if __name__ == '__main__':
    setup_database()
    print(f"Database initialized at: {DB_PATH}")
