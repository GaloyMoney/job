CREATE TABLE job_execution_control (
  id BIGINT PRIMARY KEY,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  allowed_job_types TEXT[] NOT NULL DEFAULT '{}'
);
INSERT INTO job_execution_control (id) VALUES (1);
