-- Create separate DB for Marquez lineage metadata
CREATE DATABASE marquez;
GRANT ALL PRIVILEGES ON DATABASE marquez TO airflow;
