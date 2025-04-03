drop table if exists {schema}.on_run_hook;

create table {schema}.on_run_hook (
    test_state       VARCHAR(100), -- start|end
    target_dbname    VARCHAR(100),
    target_host      VARCHAR(100),
    target_name      VARCHAR(100),
    target_schema    VARCHAR(100),
    target_type      VARCHAR(100),
    target_user      VARCHAR(100),
    target_pass      VARCHAR(100),
    target_threads   INT,
    run_started_at   VARCHAR(100),
    invocation_id    VARCHAR(100),
    thread_id        VARCHAR(100)
);