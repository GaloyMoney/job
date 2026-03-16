CREATE OR REPLACE FUNCTION notify_job_completed() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('job_completed', OLD.id::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_executions_notify_completed_trigger
AFTER DELETE ON job_executions
FOR EACH ROW
EXECUTE FUNCTION notify_job_completed();
