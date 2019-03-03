using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace AzurePGCron.VacuumSweeper
{
    enum VacuumMode
    {
        RunVacuum,
        ConfigureAgressivePerTable,
        ResetToDefault,
    }

    public class Vacuum
    {
        // Query to list all the tables in a database in the order of max transaction age.
        string vacuum_age_query = @"SELECT c.relnamespace::regnamespace::text, c.oid::regclass::text as table_name,
                                    age(c.relfrozenxid) as age FROM pg_class c WHERE c.relkind = 'r' or c.relkind = 't' ORDER BY 3 DESC;";

        // Lists all the user tables in the database
        string user_tables_query = "select quote_ident(schemaname), quote_ident(relname), 0 as age from pg_stat_user_tables;";
        
        // Internal queue to track tables to vacuum.
        ConcurrentQueue<TableToVacuum> _tblsToVacuum;
        TraceWriter log;

        // Vacuum mode. The default mode is to run the vacuum during maintenance window (UTC 12AM to UTC 6AM every day)
        VacuumMode _vacummMode;

        public Vacuum(TraceWriter log)
        {
            this.log = log;
        }

        private bool ShouldRunAgressive()
        {
            int currentHour = DateTime.UtcNow.Hour;

            int startHour = CronConfiguration.Vacuum_agressive_start_hour;
            int endHour = CronConfiguration.Run_vacuum_hours + startHour;

            if(startHour < endHour)
            {
                if (currentHour > startHour && currentHour < endHour)
                {
                    return true;
                }

                return false;
            }

            if (currentHour >= startHour || currentHour < endHour)
                return true;
            
            return false;
        }

        public void RunVacuum()
        {

            if (ShouldRunAgressive())
            {
                _vacummMode = VacuumMode.ConfigureAgressivePerTable;
                ConfigureVacuum();
            }

            if (!ShouldRunAgressive())
            {
                _vacummMode = VacuumMode.ResetToDefault;
                ConfigureVacuum();
            }

            if (CronConfiguration.Tx_wraparound_vacuum < GetMaxDatabaseAge())
            {
                _vacummMode = VacuumMode.ConfigureAgressivePerTable;
                ConfigureVacuum();
                _vacummMode = VacuumMode.RunVacuum;
                ConfigureVacuum();
            }

            if (ShouldRunAgressive() && CronConfiguration.Enable_Manual_Vacuum)
            {
                _vacummMode = VacuumMode.RunVacuum;
                ConfigureVacuum();
            }
        }

        /// <summary>
        /// This takes exclusive advisory lock to prevent two jobs running in parallel on a single server instance.
        /// </summary>
        public void ConfigureVacuum()
        {
            string lock_query = "select pg_try_advisory_lock(1000);";

            using (var conn = Server.GetSqlConnection())
            {
                var cmd = conn.GetCommand();
                cmd.CommandText = lock_query;

                object o = cmd.ExecuteScalar();

                if (!(bool)o)
                {
                    log.Info("Vacuum process in progress ...");
                    return;
                }

                _tblsToVacuum = GetTablesToVacuum();

                List<Thread> threads = new List<Thread>();
                for (int i = 0; i < CronConfiguration.Autovacuum_max_workers; i++)
                {
                    Thread t = new Thread(VacuumWorker);
                    t.Start();
                    threads.Add(t);
                }

                while (!threads.TrueForAll(t => t.ThreadState == ThreadState.Stopped))
                {
                    Thread.Sleep(10000);
                }

                cmd.CommandText = "select pg_advisory_unlock(1000)";
                o = cmd.ExecuteScalar();
            }

        }

        /// <summary>
        /// Vacuum worker that actually performs vacuum or configures the auto vacuum settings.
        /// 
        /// Note: Unfortunately Azure PostgreSQL doesn't allow configuring vacuum. Hence manually configuring overrides at the table.
        /// </summary>
        private void VacuumWorker()
        {
            TableToVacuum tbl;
            while (_tblsToVacuum.TryDequeue(out tbl))
            {
                int count = 5;
                try
                {
                    while (count > 0)
                    {
                        using (SqlConnection conn = Server.GetSqlConnection(tbl.DatabaseName))
                        {
                            var cmd = conn.GetCommand();

                            if (_vacummMode == VacuumMode.RunVacuum)
                            {
                                log.Info((string.Format("Vacuuming {0}", tbl.TableName)));
                                cmd.CommandText = string.Format("set maintenance_work_mem ='1MB';set lock_timeout='5ms';", CronConfiguration.Maintenance_work_mem_mb);
                                cmd.ExecuteNonQuery();
                                cmd.CommandText = string.Format("VACUUM {0}", tbl.TableName);
                                Console.WriteLine(cmd.CommandText);
                                cmd.ExecuteNonQuery();
                            }
                            else if (_vacummMode == VacuumMode.ConfigureAgressivePerTable)
                            {
                                cmd.CommandText = "set lock_timeout='5000ms';";
                                cmd.ExecuteNonQuery();
                                cmd.CommandText = string.Format("ALTER TABLE {0}.{1} SET (autovacuum_vacuum_cost_limit=1000, autovacuum_vacuum_cost_delay=0, autovacuum_vacuum_threshold=1000, autovacuum_vacuum_scale_factor=0);", tbl.SchemaName, tbl.TableName);
                                log.Info(cmd.CommandText);
                                cmd.ExecuteNonQuery();
                            }
                            else if (_vacummMode == VacuumMode.ResetToDefault)
                            {
                                cmd.CommandText = "set lock_timeout='5000ms';";
                                cmd.ExecuteNonQuery();
                                cmd.CommandText = string.Format("ALTER TABLE {0}.{1} SET (autovacuum_vacuum_cost_limit=200, autovacuum_vacuum_cost_delay=20, autovacuum_vacuum_threshold=50, autovacuum_vacuum_scale_factor=0.2);", tbl.SchemaName, tbl.TableName);
                                log.Info(cmd.CommandText);
                                cmd.ExecuteNonQuery();
                            }

                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    log.Info(ex.ToString());
                    count--;
                    if (count == 0)
                    {
                        _tblsToVacuum.Enqueue(tbl);
                    }
                }
            }
        }

        /// <summary>
        /// Get all the tables to vacuum.
        /// </summary>
        /// <returns></returns>
        private ConcurrentQueue<TableToVacuum> GetTablesToVacuum()
        {
            ConcurrentQueue<TableToVacuum> vacuumQueue = new ConcurrentQueue<TableToVacuum>();
            List<TableToVacuum> tblsToVacuum = new List<TableToVacuum>();
            foreach (string db in Server.GetDatabases())
            {
                using (var conn = Server.GetSqlConnection(db))
                {
                    var cmd = conn.GetCommand();

                    switch (_vacummMode)
                    {
                        case VacuumMode.RunVacuum:
                            cmd.CommandText = vacuum_age_query;
                            break;
                        case VacuumMode.ConfigureAgressivePerTable:
                        case VacuumMode.ResetToDefault:
                            cmd.CommandText = user_tables_query;
                            break;
                        default:
                            throw new Exception("Vacuum mode is not configured correctly");
                    }

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tblsToVacuum.Add(new TableToVacuum(db, reader.GetString(0), reader.GetString(1), 1));
                        }
                    }
                }
            }

            tblsToVacuum = tblsToVacuum.OrderByDescending(u => u.Priority).ToList();
            tblsToVacuum.ForEach(t => vacuumQueue.Enqueue(t));
            return vacuumQueue;
        }

        private UInt32 GetMaxDatabaseAge()
        {
            using (var conn = Server.GetSqlConnection())
            {
                var cmd = conn.GetCommand();
                cmd.CommandText = "SELECT max(age(datfrozenxid)) FROM pg_database;";
                var n = cmd.ExecuteScalar();
                return UInt32.Parse(n.ToString());
            }
        }
    }

    class TableToVacuum
    {
        public string DatabaseName { get; set; }
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public Int64 Priority { get; set; }

        public TableToVacuum(string db, string schema, string tbl, Int64 priority)
        {
            DatabaseName = db;
            SchemaName = schema;
            TableName = tbl;
            Priority = priority;
        }
    }
}
