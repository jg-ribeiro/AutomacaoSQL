-- Use este script para criar as tabelas em PostgreSQL na base sql_scheduler

-- Ative a extensão para UUID se desejar chaves UUID no futuro
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1) parameters
CREATE TABLE IF NOT EXISTS parameters (
    parameter_id    SERIAL PRIMARY KEY,
    parameter_name  TEXT    NOT NULL,
    sql_script      TEXT
);

-- 2) jobs_he
CREATE TABLE IF NOT EXISTS jobs_he (
    job_id           SERIAL PRIMARY KEY,
    job_name         TEXT    NOT NULL,
    job_status       CHAR(1) NOT NULL CHECK (job_status IN ('Y','N')),
    export_type      TEXT    NOT NULL,
    export_path      TEXT    NOT NULL,
    export_name      TEXT    NOT NULL,
    days_offset      INTEGER,
    check_parameter  CHAR(1) NOT NULL CHECK (check_parameter IN ('Y','N')),
    parameter_id     INTEGER REFERENCES parameters(parameter_id),
    data_primary_key TEXT,
    sql_script       TEXT
);

-- 3) jobs_de
CREATE TABLE IF NOT EXISTS jobs_de (
    schedule_id SERIAL PRIMARY KEY,
    job_id      INTEGER NOT NULL REFERENCES jobs_he(job_id),
    job_minute  TEXT    NOT NULL,
    job_hour    TEXT    NOT NULL,
    job_day     TEXT    NOT NULL
);

-- 4) users
CREATE TABLE IF NOT EXISTS users (
    user_id       SERIAL PRIMARY KEY,
    username      TEXT,
    password_hash TEXT,
    role          TEXT
);

-- 5) logs
CREATE TABLE IF NOT EXISTS logs (
    log_id     SERIAL PRIMARY KEY,
    timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    log_level  TEXT,
    logger_name TEXT,
    job_id     INTEGER REFERENCES jobs_he(job_id) ON DELETE SET NULL,
	user_name TEXT,
    log_text   TEXT NOT NULL,
    duration_ms INTEGER
);
