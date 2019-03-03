# AzurePGCron
Cron jobs for administering and managing Azure PostgreSQL databases.

Cron jobs runs at scheduled intervals and runs routine management and cleanup jobs.
These jobs can run in parallel.

Supported Cron Jobs:
1. Vacuum Job - this configures aggressive auto vacuum settings per table level for the specified hours and resets back to PostgreSQL engine defaults.
2. TTL Sweeper - this deletes the expired rows from the database tables.

Link - www.azuredatabases.com
