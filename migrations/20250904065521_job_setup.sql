CREATE TABLE jobs (
  id TEXT PRIMARY KEY NOT NULL,
  unique_per_type INTEGER NOT NULL DEFAULT 0,
  job_type TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX idx_unique_job_type ON jobs (job_type) WHERE unique_per_type = 1;

CREATE TABLE job_events (
  id TEXT NOT NULL REFERENCES jobs(id),
  sequence INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  event TEXT NOT NULL,
  context TEXT DEFAULT NULL,
  recorded_at TEXT NOT NULL,
  UNIQUE(id, sequence)
);

CREATE TABLE job_executions (
  id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
  job_type TEXT NOT NULL,
  queue_id TEXT,
  poller_instance_id TEXT,
  attempt_index INTEGER NOT NULL DEFAULT 1,
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'running')),
  execution_state_json TEXT,
  execute_at TEXT,
  alive_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX idx_job_executions_poller_instance
  ON job_executions(poller_instance_id)
  WHERE state = 'running';

CREATE INDEX idx_job_executions_running_alive_at
  ON job_executions(alive_at)
  WHERE state = 'running';

CREATE INDEX idx_job_executions_pending_execute_at
  ON job_executions(execute_at)
  WHERE state = 'pending';

CREATE INDEX idx_job_executions_pending_job_type_execute_at
  ON job_executions(job_type, execute_at)
  WHERE state = 'pending';

CREATE INDEX idx_job_executions_running_queue_id
  ON job_executions(queue_id)
  WHERE state = 'running' AND queue_id IS NOT NULL;
