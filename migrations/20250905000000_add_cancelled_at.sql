-- Add cancelled_at column to support cooperative job cancellation.
-- When set to a non-NULL value, a running job is marked for cancellation.

ALTER TABLE job_executions ADD COLUMN cancelled_at TIMESTAMPTZ;

-- When cancelled_at transitions from NULL to non-NULL, fire a NOTIFY so
-- the poller instance running the job can cancel it promptly.
CREATE OR REPLACE FUNCTION notify_job_cancel() RETURNS TRIGGER AS $$
BEGIN
  IF OLD.cancelled_at IS NULL AND NEW.cancelled_at IS NOT NULL THEN
    PERFORM pg_notify('job_cancel', NEW.id::text);
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_executions_notify_cancel_trigger
AFTER UPDATE ON job_executions
FOR EACH ROW
EXECUTE FUNCTION notify_job_cancel();
