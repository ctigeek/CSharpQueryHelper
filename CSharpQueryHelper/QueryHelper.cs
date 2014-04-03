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

        void NonQueryToDBWithTransaction(IEnumerable<SQLQuery> queries);
        Task NonQueryToDBWithTransactionAsync(IEnumerable<SQLQuery> queries);
        void NonQueryToDB(IEnumerable<SQLQuery> queries);
        Task NonQueryToDBAsync(IEnumerable<SQLQuery> queries);
        T ReadScalerDataFromDB<T>(string sql);
        Task<T> ReadScalerDataFromDBAsync<T>(string sql);
        T ReadScalerDataFromDB<T>(SQLQuery query);
        Task<T> ReadScalerDataFromDBAsync<T>(SQLQuery query);
        void ReadDataFromDB(IEnumerable<SQLQuery> queries);
        Task ReadDataFromDBAsync(IEnumerable<SQLQuery> queries);
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

        public void NonQueryToDBWithTransaction(IEnumerable<SQLQuery> queries)
        {
            NonQueryToDbAsync(SetQueryGroupsForSyncOperation(queries), true)
                .Wait();
        }

        public async Task NonQueryToDBWithTransactionAsync(IEnumerable<SQLQuery> queries)
        {
            await NonQueryToDbAsync(queries, true);
        }

        public void NonQueryToDB(IEnumerable<SQLQuery> queries)
        {
            NonQueryToDbAsync(SetQueryGroupsForSyncOperation(queries), false)
                .Wait();
        }

        public async Task NonQueryToDBAsync(IEnumerable<SQLQuery> queries)
        {
            await NonQueryToDbAsync(queries, false);
        }

        private async Task NonQueryToDbAsync(IEnumerable<SQLQuery> queries, bool withTransaction) {
            DbTransaction transaction = null;
            try
            {
                using (var conn = CreateConnection())
                {
                    await conn.OpenAsync();
                    bool abort = false;
                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction();
                    }
                    foreach (var groupNum in queries.Select(q => q.GroupNumber).Distinct().OrderBy(gn => gn))
                    {
                        var taskList = new Dictionary<SQLQuery, Task<int>>();
                        foreach (var query in queries.Where(q => q.GroupNumber == groupNum).OrderBy(q => q.OrderNumber))
                        {
                            query.PreQueryProcess(query);
                            var command = CreateCommand(query, conn, transaction);
                            DumpSqlAndParamsToLog(query);
                            var task = command.ExecuteNonQueryAsync();
                            taskList.Add(query, task);
                        }
                        await Task.WhenAll(taskList.Values);
                        foreach (var query in taskList.Keys)
                        {
                            query.RowCount = taskList[query].Result;
                            abort = abort || query.Postprocess(query);
                        }
                        if (abort)
                        {
                            break;
                        }
                    }
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction = null;
                    }
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

        private List<SQLQuery> SetQueryGroupsForSyncOperation(IEnumerable<SQLQuery> queries)
        {
            int groupNum = 1;
            var queryList = queries.ToList();
            foreach (var query in queryList)
            {
                query.GroupNumber = groupNum;
                query.OrderNumber = 1;
                groupNum++;
            }
            return queryList;
        }

        public T ReadScalerDataFromDB<T>(string sql)
        {
            return ReadScalerDataFromDB<T>(new SQLQuery(sql));
        }
        
        public T ReadScalerDataFromDB<T>(SQLQuery query)
        {
            var task = ReadScalerDataFromDBAsync<T>(query);
            task.Wait();
            return task.Result;
        }

        public async Task<T> ReadScalerDataFromDBAsync<T>(string sql)
        {
            return await ReadScalerDataFromDBAsync<T>(new SQLQuery(sql));
        }
        
        public async Task<T> ReadScalerDataFromDBAsync<T>(SQLQuery query)
        {
            var conn = CreateConnection();
            T returnResult = default(T);
            using (conn)
            {
                await conn.OpenAsync();
                var command = CreateCommand(query, conn);
                query.PreQueryProcess(query); 
                DumpSqlAndParamsToLog(query);
                var result = await command.ExecuteScalarAsync();
                query.RowCount = 1;
                query.Postprocess(query);
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

        public async Task ReadDataFromDBAsync(IEnumerable<SQLQuery> queries)
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

        public void ReadDataFromDB(IEnumerable<SQLQuery> queries)
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

        //private void ProcessIdentityColumn(NonQueryWithParameters query, DbConnection conn, DbTransaction transaction = null)
        //{
        //    if (query.SetPrimaryKey != null)
        //    {
        //        var identity = GetIdentity(conn, transaction);
        //        query.SetPrimaryKey(identity, query);
        //    }
        //}

        private void AddParameters(DbCommand command, SQLQuery query)
        {
            foreach (string paramName in query.InParameters.Keys)
            {
                if (query.InParameters[paramName] != null && query.InParameters[paramName].Any() && query.ModifiedSQL.Contains("@" + paramName))
                {
                    var parameterDictionary = new Dictionary<string, object>();
                    foreach (var value in query.InParameters[paramName])
                    {
                        parameterDictionary.Add(string.Format("{0}{1}", paramName, parameterDictionary.Count), value);
                    }
                    //TODO: this needs to be a regex otherwise it could be prone to replacing paramName that starts with the same name eg. @myvar & @myvar2.
                    query.ModifiedSQL = query.ModifiedSQL.Replace("@" + paramName, string.Join(",", parameterDictionary.Select(pd => "@" + pd.Key)));
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
    
    public class SQLQuery {
        public SQLQuery(string sql) {
            this.OriginalSQL = sql;
            this.ModifiedSQL = OriginalSQL;
            Parameters = new Dictionary<string, object>();
            InParameters = new Dictionary<string, List<object>>();
            GroupNumber = 0;
            OrderNumber = 0;
        }
        public readonly Dictionary<string, object> Parameters;
        public readonly Dictionary<string, List<object>> InParameters;
        public readonly string OriginalSQL;
        public string ModifiedSQL { get; set; }
        public int RowCount { get; set; }
        public int GroupNumber { get; set; }
        public int OrderNumber { get; set; }
        private Action<SQLQuery> preQueryProcess;
        public Action<SQLQuery> PreQueryProcess
        {
            get
            {
                if (preQueryProcess == null)
                {
                    preQueryProcess = new Action<SQLQuery>(query => { });
                }
                return preQueryProcess;
            }
            set
            {
                preQueryProcess = value;
            }
        }
        private Func<DbDataReader, bool> processRow;
        public Func<DbDataReader, bool> ProcessRow {
            get
            {
                if (processRow == null)
                {
                    processRow = new Func<DbDataReader, bool>(dr => { return true; });
                }
                return processRow;
            }
            set
            {
                processRow = value;
            }
        }
        private Func<SQLQuery, bool> postprocess;
        public Func<SQLQuery, bool> Postprocess
        {
            get
            {
                if (postprocess == null)
                {
                    postprocess = new Func<SQLQuery, bool>(query => { return true; });
                }
                return postprocess;
            }
            set
            {
                postprocess = value;
            }
        }
    }
    //public class SQLQuery<T> : SQLQuery
    //{
    //    public readonly T Graph;
    //    public SQLQuery(string sql, T graph)
    //        : base(sql)
    //    {
    //        if (graph == null)
    //        {
    //            throw new ArgumentNullException("graph");
    //        }
    //        this.Graph = graph;
    //    }
    //    public Func<SQLQuery, T, bool> preQueryProcess
    //    {
    //        get
    //        {
    //            return 
    //        }
    //    }
    //}


    //public class SQLQueryWithParameters : SQLQuery
    //{
    //    public SQLQueryWithParameters(string sql)
    //        : this(sql, null)
    //    { }

    //    public SQLQueryWithParameters(string sql,Func<DbDataReader, bool> processRow)
    //        : base(sql)
    //    {
    //        if (processRow == null)
    //        {
    //            this.ProcessRow = new Func<DbDataReader, bool>(dr => { return true; });
    //        }
    //        this.ProcessRow = processRow;
    //    }
    //    public readonly Func<DbDataReader, bool> ProcessRow;
    //}
    //public class NonQueryWithParameters : SQLQuery
    //{
    //    public NonQueryWithParameters(string sql)
    //        : base(sql)
    //    {
    //    }
    //    public Action<int, NonQueryWithParameters> SetPrimaryKey { get; set; }
    //    public int Order { get; set; }
    //    public object Tag { get; set; }
    //    public string IdentitySql { get; set; }

    //}
}
