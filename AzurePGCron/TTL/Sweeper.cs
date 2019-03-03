using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Npgsql.Schema;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using AzurePGCron;

namespace AzurePGCron.TTLSweeper
{
    /// <summary>
    /// Sweeper class that cleans up the database tables at the scheduled intervals.
    /// </summary>
    class Sweeper
    {
        string _queryFormat = "delete from {0}.{1} where (EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM {2})) > {3};";

        TraceWriter _traceWriter;

        public Sweeper(TraceWriter traceWriter)
        {
            _traceWriter = traceWriter;
        }

        public void RunSweeper()
        {
            List<TTLTable> tables = TTLTable.GetTTLTables();
            _traceWriter.Info(string.Format("Processing {0} tables on the server", tables.Count));

            foreach (TTLTable tbl in tables)
            {
                try
                {
                    using (SqlConnection conn = new SqlConnection())
                    {
                        NpgsqlCommand cmd = conn.GetConnection().CreateCommand();
                        cmd.CommandText = string.Format(_queryFormat, tbl.SchemaName, tbl.TableName, tbl.ColumnName, tbl.ExpiryTimeInSeconds);
                        int delRows = cmd.ExecuteNonQuery();
                        _traceWriter.Info(string.Format("Delete {0} rows from {1}.{2}", delRows, tbl.SchemaName, tbl.TableName));
                    }
                }
                catch(Exception ex)
                {
                    _traceWriter.Error(ex.ToString());
                }
            }
        }
    }

    class TTLTable
    {
        string _databaseName;
        string _schemaName;
        string _tableName;
        string _columnName;
        int _expiryTime;

        /// <summary>
        /// Collects the tables to cleanup
        /// </summary>
        /// <returns></returns>
        public static List<TTLTable> GetTTLTables()
        {
            List<TTLTable> tables = new List<TTLTable>();

            using (SqlConnection sqlConn = Server.GetSqlConnection())
            {
                NpgsqlConnection conn = sqlConn.GetConnection();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "Select database_name, schema_name, table_name, col_name, expiry_in_sec from pg_ttl_tables;";

                    using (var reader = cmd.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            TTLTable tbl = new TTLTable();
                            tbl._databaseName = reader.GetString(0);
                            tbl._schemaName = reader.GetString(1);
                            tbl._tableName = reader.GetString(2);
                            tbl._columnName = reader.GetString(3);
                            tbl._expiryTime = reader.GetInt32(4);
                            tables.Add(tbl);
                        }
                    }
                }
            }

            return tables;
        }

        public string Databasename
        {
            get
            {
                return _databaseName;
            }
        }

        public string SchemaName
        {
            get
            {
                return _schemaName;
            }
        }

        public string TableName
        {
            get
            {
                return _tableName;
            }
        }

        public string ColumnName
        {
            get
            {
                return _columnName;
            }
        }

        public int ExpiryTimeInSeconds
        {
            get
            {
                return _expiryTime;
            }
        }
    }
}
