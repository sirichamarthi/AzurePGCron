using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using System.Data;
using System.Data.Entity;

namespace AzurePGCron
{
    /// <summary>
    /// CronConfiguration class
    /// </summary>
    public class CronConfiguration
    {
        static int idle_session_timeout = 900;
        static double autovacuum_vacuum_analyze_scale_factor = 0.05;
        static int autovacuum_vacuum_cost_delay = 10;
        static int autovacuum_vacuum_cost_limit = 1000;
        static double autovacuum_vacuum_scale_factor = 0.01;
        static int autovacuum_vacuum_threshold = 1000;
        static int autovacuum_max_workers = 5;
        static int tx_wraparound_vacuum = 750000000;
        static int maintenance_work_mem_mb = 1024;
        static int vacuum_schedule_hour = 0;
        static int run_vacuum_hours = 6;
        static bool enable_manual_vacuum = true;
        static bool enable_ttl_sweeper = true;

        /// <summary>
        /// Enables TTL sweeper that clears the unwanted rows from the tables.
        /// Please populate pg_ttl_tables for selecting the tables to cleanup.
        /// </summary>
        public static bool Enable_TTL_Sweeper { get => enable_ttl_sweeper; }

        /// <summary>
        /// Run manual vacuum on each table in the database. Manual vacuum is required when auto vacuum is disabled on the table.
        /// </summary>
        public static bool Enable_Manual_Vacuum { get => enable_manual_vacuum; }

        /// <summary>
        /// Kills the idle sessions connected to the server. This helps freeing up the server resources and also let vacuum collect dead tuples.
        /// </summary>
        public static int Idle_session_timeout { get => idle_session_timeout; }

        /// <summary>
        /// Vacuum analyze scale factor. Please refer to PostgreSQL documentation.
        /// </summary>
        public static double Autovacuum_vacuum_analyze_scale_factor { get => autovacuum_vacuum_analyze_scale_factor; }

        /// <summary>
        /// Please refer to PostgreSQL documentation.
        /// </summary>
        public static int Autovacuum_vacuum_cost_delay { get => autovacuum_vacuum_cost_delay; }

        /// <summary>
        /// Please refer to PostgreSQL documentation 
        /// </summary>
        public static int Autovacuum_vacuum_cost_limit { get => autovacuum_vacuum_cost_limit; }

        /// <summary>
        /// Please refer to PostgreSQL documentation
        /// </summary>
        public static double Autovacuum_vacuum_scale_factor { get => autovacuum_vacuum_scale_factor; }

        /// <summary>
        /// Please refer to PostgreSQL documentation
        /// </summary>
        public static int Autovacuum_vacuum_threshold { get => autovacuum_vacuum_threshold; }

        /// <summary>
        /// Please refer to PostgreSQL documentation
        /// </summary>
        public static int Autovacuum_max_workers { get => autovacuum_max_workers; }

        /// <summary>
        /// Please refer to PostgreSQL documentation
        /// </summary>
        public static int Tx_wraparound_vacuum { get => tx_wraparound_vacuum; }

        /// <summary>
        /// Please refer to PostgreSQL documentation
        /// </summary>
        public static int Maintenance_work_mem_mb { get => maintenance_work_mem_mb; }

        /// <summary>
        /// Time at which manual vacuum / agressive auto vacuum starts running.
        /// </summary>
        public static int Vacuum_agressive_start_hour { get => vacuum_schedule_hour; }

        /// <summary>
        /// After Vacuum_agressive_start_hour + Run_vacuum_hours, auto vacuum truns into non-agressive mode.
        /// </summary>
        public static int Run_vacuum_hours { get => run_vacuum_hours; }

        public static void CreateConfig()
        {
            using (SqlConnection conn = Server.GetSqlConnection())
            {
                var cmd = conn.GetCommand();
                cmd.CommandText = "CREATE TABLE  IF NOT EXISTS pg_cron_config(name varchar(200), setting varchar(20));";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Loads the cluster configuration
        /// </summary>
        public static void LocadConfig()
        {
            string query = "select name, setting from pg_cron_config;";
            CreateConfig();
            using (SqlConnection conn = Server.GetSqlConnection())
            {
                var cmd = conn.GetCommand();
                cmd.CommandText = query;
                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                    {
                        switch (r.GetString(0))
                        {
                            case "idle_session_timeout":
                                idle_session_timeout = Int32.Parse(r.GetString(1));
                                break;
                            case "autovacuum_vacuum_analyze_scale_factor":
                                autovacuum_vacuum_analyze_scale_factor = Double.Parse(r.GetString(1));
                                break;
                            case "autovacuum_vacuum_cost_delay":
                                autovacuum_vacuum_cost_delay = Int32.Parse(r.GetString(1));
                                break;
                            case "autovacuum_vacuum_cost_limit":
                                autovacuum_vacuum_cost_limit = Int32.Parse(r.GetString(1));
                                break;
                            case "autovacuum_vacuum_threshold":
                                autovacuum_vacuum_threshold = Int32.Parse(r.GetString(1));
                                break;
                            case "autovacuum_vacuum_scale_factor":
                                autovacuum_vacuum_scale_factor = Double.Parse(r.GetString(1));
                                break;
                            case "autovacuum_max_workers":
                                autovacuum_max_workers = Int32.Parse(r.GetString(1));
                                break;
                            case "tx_wraparound_vacuum":
                                tx_wraparound_vacuum = Int32.Parse(r.GetString(1));
                                break;
                            case "maintenance_work_mem_mb":
                                maintenance_work_mem_mb = Int32.Parse(r.GetString(1));
                                break;
                            case "vacuum_schedule_hour":
                                vacuum_schedule_hour = Int32.Parse(r.GetString(1));
                                break;
                            case "vacuum_agressive_disable_hour":
                                run_vacuum_hours = Int32.Parse(r.GetString(1));
                                break;
                            case "enable_ttl_sweeper":
                                enable_ttl_sweeper = Boolean.Parse(r.GetString(1));
                                break;
                            case "enable_manual_vacuum":
                                enable_manual_vacuum = Boolean.Parse(r.GetString(1));
                                break;
                        }
                    }
                }
            }
        }
    }
}
