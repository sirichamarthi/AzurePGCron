 CREATE TABLE IF NOT EXISTS pg_cron_config(name varchar(200) primary key, setting varchar(100));

 -- Add the default configurations
 INSERT INTO pg_cron_config VALUES('vacuum_schedule_hour', '0');
 INSERT INTO pg_cron_config VALUES('idle_session_timeout', '900');
 INSERT INTO pg_cron_config VALUES('autovacuum_vacuum_analyze_scale_factor', '0.05');
 INSERT INTO pg_cron_config VALUES('autovacuum_vacuum_cost_delay', '10');
 INSERT INTO pg_cron_config VALUES('autovacuum_vacuum_cost_limit', '1000');
 INSERT INTO pg_cron_config VALUES('autovacuum_vacuum_scale_factor', '0.01');
 INSERT INTO pg_cron_config VALUES('autovacuum_vacuum_threshold', '1000');
 INSERT INTO pg_cron_config VALUES('autovacuum_max_workers', '5');
 INSERT INTO pg_cron_config VALUES('tx_wraparound_vacuum', '750000000');
 INSERT INTO pg_cron_config VALUES('maintenance_work_mem_mb', '1024');
 INSERT INTO pg_cron_config VALUES('run_vacuum_hours', '6');
 INSERT INTO pg_cron_config VALUES('enable_manual_vacuum', 'false');
 INSERT INTO pg_cron_config VALUES('enable_ttl_sweeper', 'false');

 -- Configure TTL Sweeper
 CREATE TABLE IF NOT EXISTS pg_ttl_tables(database_name name, schema_name name, table_name name, col_name name, expiry_in_sec int, primary key(database_name, schema_name, table_name, col_name));
 INSERT INTO pg_cron_config VALUES('enable_ttl_sweeper', 'true');
 -- example
 CREATE TABLE IF NOT EXISTS test_tbl(id int primary key, col1 timestamp, col2 text);
 INSERT INTO pg_ttl_tables values('postgres', 'public','test_tbl','col1', 10);