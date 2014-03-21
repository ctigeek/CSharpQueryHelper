using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CSharpQueryHelper
{
    public interface IQueryHelper
    {
        Action<string, System.Diagnostics.TraceEventType> LogMessage { get; set; }
        bool DebugLoggingEnabled { get; set; }
        void NonQueryToDBWithTransaction(IEnumerable<NonQueryWithParameters> queries);
        Task NonQueryToDBWithTransactionAsync(IEnumerable<NonQueryWithParameters> queries);
        void NonQueryToDB(NonQueryWithParameters query);
        Task NonQueryToDBAsync(NonQueryWithParameters query);
        T ReadScalerDataFromDB<T>(string sql);
        Task<T> ReadScalerDataFromDBAsync<T>(string sql);
        T ReadScalerDataFromDB<T>(SQLQueryWithParameters query);
        Task<T> ReadScalerDataFromDBAsync<T>(SQLQueryWithParameters query);
        void ReadDataFromDB(SQLQueryWithParameters query);
        Task ReadDataFromDBAsync(SQLQueryWithParameters query);
    }

    public class QueryHelper : IQueryHelper
    {
        public readonly string ConnectionString;
        public readonly string DataProvider;
        public readonly DbProviderFactory DbFactory;
        public bool DebugLoggingEnabled { get; set; }
        public Action<string,System.Diagnostics.TraceEventType> LogMessage { get; set; }

        public QueryHelper(string connectionString, string dataProvider, DbProviderFactory dbFactory)
        {
            if (dbFactory == null) throw new ArgumentException("dbFactory not found.", "dbFactory");
            this.ConnectionString = connectionString;
            this.DataProvider = dataProvider;
            this.DbFactory = dbFactory;
            DebugLoggingEnabled = false;
        }

        public QueryHelper(string connectionString, string dataProvider) :
            this(connectionString, dataProvider, DbProviderFactories.GetFactory(dataProvider))
        {
        }

        public QueryHelper(string connectionName)
        {
            if (ConfigurationManager.ConnectionStrings[connectionName] == null)
            {
                throw new ArgumentException("'"+ connectionName + "' is not a valid connection string name. Check your configuration file to make sure it exists.", "connectionName");
            }
            this.ConnectionString = ConfigurationManager.ConnectionStrings[connectionName].ConnectionString;
            this.DataProvider = ConfigurationManager.ConnectionStrings[connectionName].ProviderName;
            this.DbFactory = DbProviderFactories.GetFactory(DataProvider);
            DebugLoggingEnabled = false;
        }

        public void NonQueryToDBWithTransaction(IEnumerable<NonQueryWithParameters> queries)
        {
            DbTransaction transaction = null;
            try
            {
                using (var conn = CreateConnection())
                {
                    conn.Open();
                    transaction = conn.BeginTransaction();
                    foreach (var query in queries.OrderBy(q=>q.Order))
                    {
                        var command = CreateCommand(query, conn, transaction);
                        DumpSqlAndParamsToLog(query);
                        query.RowCount = command.ExecuteNonQuery();
                        ProcessIdentityColumn(query, conn, transaction);
                    }
                    transaction.Commit();
                    transaction = null;
                    conn.Close();
                }
            }
            catch (Exception)
            {
                try
                {
                    if (transaction != null)
                    {
                        transaction.Rollback();
                    }
                }
                catch { }
                throw;
            }
        }

        public async Task NonQueryToDBWithTransactionAsync(IEnumerable<NonQueryWithParameters> queries)
        {
            DbTransaction transaction = null;
            try
            {
                using (var conn = CreateConnection())
                {
                    await conn.OpenAsync();
                    transaction = conn.BeginTransaction();
                    foreach (var query in queries.OrderBy(q => q.Order))
                    {
                        var command = CreateCommand(query, conn, transaction);
                        DumpSqlAndParamsToLog(query);
                        query.RowCount = await command.ExecuteNonQueryAsync();
                        await ProcessIdentityColumnAsync(query, conn, transaction);
                    }
                    transaction.Commit();
                    transaction = null;
                    conn.Close();
                }
            }
            catch (Exception)
            {
                try
                {
                    if (transaction != null)
                    {
                        transaction.Rollback();
                    }
                }
                catch { }
                throw;
            }
        }

        public async Task NonQueryToDBAsync(NonQueryWithParameters query)
        {
            query.RowCount = 0;
            using (var conn = CreateConnection())
            {
                await conn.OpenAsync();
                var command = CreateCommand(query, conn);
                DumpSqlAndParamsToLog(query);
                query.RowCount = await command.ExecuteNonQueryAsync();
                await ProcessIdentityColumnAsync(query, conn);
                conn.Close();
            }
        }

        public void NonQueryToDB(NonQueryWithParameters query)
        {
            query.RowCount = 0;
            using (var conn = CreateConnection())
            {
                conn.Open();
                var command = CreateCommand(query, conn);
                DumpSqlAndParamsToLog(query);
                query.RowCount = command.ExecuteNonQuery();
                ProcessIdentityColumn(query, conn);
                conn.Close();
            }
        }

        public T ReadScalerDataFromDB<T>(string sql)
        {
            return ReadScalerDataFromDB<T>(new SQLQueryWithParameters(sql, null));
        }
        
        public T ReadScalerDataFromDB<T>(SQLQueryWithParameters query)
        {
            var conn = CreateConnection();
            T returnResult = default(T);
            using (conn)
            {
                conn.Open();
                var command = CreateCommand(query, conn);
                DumpSqlAndParamsToLog(query);
                var result = command.ExecuteScalar();
                query.RowCount = 1;
                returnResult = ProcessScalerResult<T>(result);
                conn.Close();
            }
            return returnResult;
        }

        public async Task<T> ReadScalerDataFromDBAsync<T>(string sql)
        {
            return await ReadScalerDataFromDBAsync<T>(new SQLQueryWithParameters(sql, null));
        }
        
        public async Task<T> ReadScalerDataFromDBAsync<T>(SQLQueryWithParameters query)
        {
            var conn = CreateConnection();
            T returnResult = default(T);
            using (conn)
            {
                await conn.OpenAsync();
                var command = CreateCommand(query, conn);
                DumpSqlAndParamsToLog(query);
                var result = await command.ExecuteScalarAsync();
                query.RowCount = 1;
                returnResult = ProcessScalerResult<T>(result);
                conn.Close();
            }
            return returnResult;
        }
        
        public T ProcessScalerResult<T>(object result)
        {
            T returnResult = default(T);
            if (result != DBNull.Value)
            {
                if (result is T)
                {
                    returnResult = (T)result;
                }
                else if (typeof(T) == typeof(string))
                {
                    object stringResult = (object)result.ToString();
                    returnResult = (T)stringResult;
                }
            }
            return returnResult;
        }

        public async Task ReadDataFromDBAsync(SQLQueryWithParameters query)
        {
            query.RowCount = 0;
            using (var conn = CreateConnection())
            {
                await conn.OpenAsync();
                
                var command = CreateCommand(query, conn);
                DumpSqlAndParamsToLog(query);
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        query.RowCount++;
                        if (!query.ProcessRow(reader))
                        {
                            break;
                        }
                    }
                    reader.Close();
                }
                conn.Close();
            }
        }

        public void ReadDataFromDB(SQLQueryWithParameters query)
        {
            query.RowCount = 0;
            using (var conn = CreateConnection())
            {
                conn.Open();
                var command = CreateCommand(query, conn);
                DumpSqlAndParamsToLog(query);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        query.RowCount++;
                        if (!query.ProcessRow(reader))
                        {
                            break;
                        }
                    }
                    reader.Close();
                }
                conn.Close();
            }
        }

        private void ProcessIdentityColumn(NonQueryWithParameters query, DbConnection conn, DbTransaction transaction = null)
        {
            if (query.SetPrimaryKey != null)
            {
                var identity = GetIdentity(conn, transaction);
                query.SetPrimaryKey(identity, query);
            }
        }

        private async Task ProcessIdentityColumnAsync(NonQueryWithParameters query, DbConnection conn, DbTransaction transaction = null)
        {
            if (query.SetPrimaryKey != null)
            {
                var identity = await GetIdentityAsync(conn, transaction);
                query.SetPrimaryKey(identity, query);
            }
        }

        private int GetIdentity(DbConnection connection, DbTransaction transaction = null)
        {
            DbCommand command;
            var sql = GetIdentitySql();
            var query = new SQLQueryWithParameters(sql);
            command = CreateCommand(query, connection, transaction);
            LogDebug("About to execute \"" + sql +"\".");
            object result = command.ExecuteScalar();
            int identity = GetIdentityFromResult(result);
            return identity;
        }

        private async Task<int> GetIdentityAsync(DbConnection connection, DbTransaction transaction = null)
        {
            DbCommand command;
            var sql = GetIdentitySql();
            var query = new SQLQueryWithParameters(sql);
            command = CreateCommand(query, connection, transaction);
            LogDebug("About to execute \"" + sql + "\".");
            object result = await command.ExecuteScalarAsync();
            int identity = GetIdentityFromResult(result);
            return identity;
        }

        private int GetIdentityFromResult(object result)
        {
            if (result != DBNull.Value)
            {
                if (result is System.Decimal)
                {
                    decimal id = (decimal)result;
                    return (int)id;
                }
                else if (result is Int32)
                {
                    return (int)result;
                }
                else if (result is Int64)
                {
                    return (int)result;
                }
                else
                {
                    return int.Parse(result.ToString());
                }
            }
            return -1;
        }

        private string GetIdentitySql()
        {
            var sql = string.Empty;

            if (this.DataProvider.Contains("SqlServerCe"))
            {
                sql = "SELECT @@IDENTITY;";
            }
            else if (this.DataProvider.Contains("SqlServer"))
            {
                sql = "SELECT SCOPE_IDENTITY();";
            }
            else if (this.DataProvider.Contains("SQLite"))
            {
                sql = "SELECT last_insert_rowid();";
            }
            else
            {
                throw new ApplicationException("Unknown provider type for retrieving identity column.");
            }
            return sql;
        }

        private void AddParameters(DbCommand command, SQLQuery query)
        {
            if (query.BuildParameters != null)
            {
                query.BuildParameters(query);
            }
            foreach (string paramName in query.InParameters.Keys)
            {
                if (query.InParameters[paramName] != null && query.InParameters[paramName].Any() && query.ModifiedSQL.Contains("@"+paramName))
                {
                    var parameterDictionary = new Dictionary<string, object>();
                    foreach (var value in query.InParameters[paramName])
                    {
                        parameterDictionary.Add(string.Format("{0}{1}", paramName, parameterDictionary.Count), value);
                    }
                    query.ModifiedSQL = query.ModifiedSQL.Replace("@" + paramName, string.Join(",", parameterDictionary.Select(pd => "@"+pd.Key)));
                    foreach (var parameter in parameterDictionary.Keys)
                    {
                        query.Parameters.Add(parameter, parameterDictionary[parameter]);
                    }
                }
            }
            foreach (string paramName in query.Parameters.Keys)
            {
                command.Parameters.Add(CreateParameter(paramName, query.Parameters[paramName]));
            }
        }

        private DbParameter CreateParameter(string name, object value)
        {
            var parameter = DbFactory.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }

        private DbConnection CreateConnection()
        {
            var connection = DbFactory.CreateConnection();
            connection.ConnectionString = ConnectionString;
            return connection;
        }

        private DbCommand CreateCommand(SQLQuery query, DbConnection connection, DbTransaction transaction = null)
        {
            var command = DbFactory.CreateCommand();
            command.Connection = connection;
            AddParameters(command, query);
            command.CommandText = query.ModifiedSQL;
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            return command;
        }

        private void DumpSqlAndParamsToLog(SQLQuery query)
        {
            if (LogMessage == null || !DebugLoggingEnabled) return;

            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            sb.AppendFormat("About to execute \"{0}\" ", query.ModifiedSQL);
            if (query.Parameters.Count > 0)
            {
                sb.Append(" with parameters:\r\n");
                foreach (string key in query.Parameters.Keys)
                {
                    sb.AppendFormat("{0}={1} with type {2}.\r\n", key, query.Parameters[key], query.Parameters[key].GetType());
                }
            }
            else
            {
                sb.Append(" with no parameters.");
            }
            LogDebug(sb.ToString());
        }

        private void LogWarn(string message)
        {
            if (LogMessage != null)
            {
                LogMessage(message, System.Diagnostics.TraceEventType.Warning);
            }
        }
        private void LogError(string message)
        {
            if (LogMessage != null)
            {
                LogMessage(message, System.Diagnostics.TraceEventType.Error);
            }
        }
        private void LogDebug(string message)
        {
            if (LogMessage != null && DebugLoggingEnabled)
            {
                LogMessage(message, System.Diagnostics.TraceEventType.Verbose);
            }
        }
    }
    
    public abstract class SQLQuery {
        public SQLQuery(string sql) {
            this.OriginalSQL = sql;
            this.ModifiedSQL = OriginalSQL;
            Parameters = new Dictionary<string, object>();
            InParameters = new Dictionary<string, List<object>>();
        }
        public readonly Dictionary<string, object> Parameters;
        public readonly Dictionary<string, List<object>> InParameters;
        public Action<SQLQuery> BuildParameters { get; set; }
        public readonly string OriginalSQL;
        public string ModifiedSQL { get; set; }
        public int RowCount { get; set; }
    }
    public class SQLQueryWithParameters : SQLQuery
    {
        public SQLQueryWithParameters(string sql)
            : this(sql, null)
        { }

        public SQLQueryWithParameters(string sql,Func<DbDataReader, bool> processRow)
            : base(sql)
        {
            if (processRow == null)
            {
                this.ProcessRow = new Func<DbDataReader, bool>(dr => { return true; });
            }
            this.ProcessRow = processRow;
        }
        public readonly Func<DbDataReader, bool> ProcessRow;
    }
    public class NonQueryWithParameters : SQLQuery
    {
        public NonQueryWithParameters(string sql)
            : base(sql)
        {
        }
        public Action<int, NonQueryWithParameters> SetPrimaryKey { get; set; }
        public int Order { get; set; }
        public object Tag { get; set; }
    }
}
