using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace AzurePGCron
{
    static class Server
    {
        public static SqlConnection GetSqlConnection(string database = "postgres")
        {
            return new SqlConnection(database);
        }

        public static List<string> GetDatabases(string database = "postgres")
        {
            List<string> dbs = new List<string>();

            using (SqlConnection srvconn = GetSqlConnection())
            {
                var cmd = srvconn.GetCommand();
                cmd.CommandText = "select quote_ident(datname) from pg_database where datname not in ('template0', 'template1', 'azure_maintenance')";

                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                    {
                        dbs.Add(r.GetString(0));
                    }
                }
            }

            return dbs;
        }

        public static List<string> GetUserTables(string database = "postgres")
        {
            List<string> tbls = new List<string>();

            using (SqlConnection srvconn = GetSqlConnection())
            {
                var cmd = srvconn.GetCommand();
                cmd.CommandText = "select quote_ident(relname), quote_ident(schemaname) from pg_stat_user_tables";

                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                    {
                        tbls.Add(string.Format("{0}.{1}", r.GetString(0), r.GetString(1)));
                    }
                }
            }

            return tbls;
        }
    }



    class SqlConnection : IDisposable
    {
        string _database = "";
        string _connString;
        public SqlConnection(string db = "postgres")
        {
            _database = db;
            _connString = string.Format(Environment.GetEnvironmentVariable("postgres_connection"), db);
        }

        NpgsqlConnection _conn = null;
        public NpgsqlConnection GetConnection()
        {
            if (_conn != null && _conn.State == System.Data.ConnectionState.Open)
            {
                return _conn;
            }

            _conn = new NpgsqlConnection(_connString);
            _conn.Open();
            return _conn;
        }

        public NpgsqlCommand GetCommand()
        {
            return GetConnection().CreateCommand();
        }

        public void Dispose()
        {
            _conn.Close();
        }
    }
}
