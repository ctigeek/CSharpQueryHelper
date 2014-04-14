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
        
        void RunQuery(SQLQuery query);
        void RunQuery(IEnumerable<SQLQuery> queries, bool withTransaction = false);
        Task RunQueryAsync(IEnumerable<SQLQuery> queries, bool withTransaction = false);

        T ReadScalerDataFromDB<T>(string sql);
        Task<T> ReadScalerDataFromDBAsync<T>(string sql);
        T ReadScalerDataFromDB<T>(SQLQuery query);
        Task<T> ReadScalerDataFromDBAsync<T>(SQLQuery query);
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

        public void RunQuery(SQLQuery query)
        {
            RunQuery(new [] { query });
        }
        public void RunQuery(IEnumerable<SQLQuery> queries, bool withTransaction = false)
        {
            RunQueryAsync(queries, withTransaction)
                .Wait();
        }

        public async Task RunQueryAsync(SQLQuery query)
        {
            await RunQueryAsync(new[] { query });
        }
        public async Task RunQueryAsync(IEnumerable<SQLQuery> queries, bool withTransaction = false)
        {
            DbTransaction transaction = null;
            try
            {
                using (var conn = CreateConnection())
                {
                    await conn.OpenAsync();
                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction();
                    }
                    foreach (var groupNum in queries.Select(q => q.GroupNumber).Distinct().OrderBy(gn => gn))
                    {
                        var taskList = new List<QueryTask>();
                        foreach (var query in queries.Where(q => q.GroupNumber == groupNum).OrderBy(q => q.OrderNumber))
                        {
                            var queryTask = ExecuteQuery(query, conn, transaction);
                            taskList.Add(queryTask);
                        }
                        await Task.WhenAll(taskList.Select(qt => qt.Task));
                        foreach (var queryTask in taskList) 
                        {
                            if (queryTask.Query.SQLQueryType == SQLQueryType.DataReader)
                            {
                                await ProcessReadQueryAsync(queryTask);
                            }
                            else
                            {
                                ProcessQuery(queryTask);
                            }
                            if (queryTask.Query.PostQueryProcess != null)
                            {
                                queryTask.Query.CausedAbort = !queryTask.Query.PostQueryProcess(queryTask.Query);
                            }
                        }
                        if (taskList.Exists(qt => qt.Query.CausedAbort == true))
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

        private async Task ProcessReadQueryAsync(QueryTask queryTask)
        {
            if (queryTask.Query.SQLQueryType == SQLQueryType.DataReader)
            {
                queryTask.Query.RowCount = 0;
                using (var reader = queryTask.ReaderTask.Result)
                {
                    while (await reader.ReadAsync())
                    {
                        queryTask.Query.RowCount++;
                        if (!queryTask.Query.ProcessRow(reader))
                        {
                            break;
                        }
                    }
                    reader.Close();
                }
            }
        }
        
        private void ProcessQuery(QueryTask queryTask)
        {
            if (queryTask.Query.SQLQueryType == SQLQueryType.NonQuery)
            {
                queryTask.Query.RowCount = queryTask.NonQueryTask.Result;
            }
            else if (queryTask.Query.SQLQueryType == SQLQueryType.Scaler)
            {
                queryTask.Query.RowCount = 1;
                var result = queryTask.ScalerTask.Result;
                if (queryTask.Query is ScalerQuery)
                {
                    ScalerQuery scalerQuery = (ScalerQuery)queryTask.Query;
                    scalerQuery.ProcessScalerResult(result);
                }
            }
        }

        private QueryTask ExecuteQuery(SQLQuery query, DbConnection connection, DbTransaction transaction)
        {
            if (query.PreQueryProcess != null)
            {
                query.PreQueryProcess(query);
            }
            var command = CreateCommand(query, connection, transaction);
            DumpSqlAndParamsToLog(query);
            if (query.SQLQueryType == SQLQueryType.NonQuery)
            {
                var task = command.ExecuteNonQueryAsync();
                query.Executed = true;
                return new QueryTask(query, task);
            }
            else if (query.SQLQueryType == SQLQueryType.DataReader)
            {
                var task = command.ExecuteReaderAsync();
                query.Executed = true;
                return new QueryTask(query, task);
            }
            else
            {
                var task = command.ExecuteScalarAsync();
                query.Executed = true;
                return new QueryTask(query, task);
            }
        }









        //public void NonQueryToDBWithTransaction(IEnumerable<SQLQuery> queries)
        //{
        //    NonQueryToDbAsync(SetQueryGroupsForSyncOperation(queries), true)
        //        .Wait();
        //}

        //public async Task NonQueryToDBWithTransactionAsync(IEnumerable<SQLQuery> queries)
        //{
        //    await NonQueryToDbAsync(queries, true);
        //}

        //public void NonQueryToDB(IEnumerable<SQLQuery> queries)
        //{
        //    NonQueryToDbAsync(SetQueryGroupsForSyncOperation(queries), false)
        //        .Wait();
        //}

        //public async Task NonQueryToDBAsync(IEnumerable<SQLQuery> queries)
        //{
        //    await NonQueryToDbAsync(queries, false);
        //}

        //private async Task NonQueryToDbAsync(IEnumerable<SQLQuery> queries, bool withTransaction) {
        //    DbTransaction transaction = null;
        //    try
        //    {
        //        using (var conn = CreateConnection())
        //        {
        //            await conn.OpenAsync();
        //            bool abort = false;
        //            if (withTransaction)
        //            {
        //                transaction = conn.BeginTransaction();
        //            }
        //            foreach (var groupNum in queries.Select(q => q.GroupNumber).Distinct().OrderBy(gn => gn))
        //            {
        //                var taskList = new Dictionary<SQLQuery, Task<int>>();
        //                foreach (var query in queries.Where(q => q.GroupNumber == groupNum).OrderBy(q => q.OrderNumber))
        //                {
        //                    query.PreQueryProcess(query);
        //                    var command = CreateCommand(query, conn, transaction);
        //                    DumpSqlAndParamsToLog(query);
        //                    var task = command.ExecuteNonQueryAsync();
        //                    taskList.Add(query, task);
        //                }
        //                await Task.WhenAll(taskList.Values);
        //                foreach (var query in taskList.Keys)
        //                {
        //                    query.RowCount = taskList[query].Result;
        //                    abort = abort || !query.PostQueryProcess(query);
        //                }
        //                if (abort)
        //                {
        //                    break;
        //                }
        //            }
        //            if (transaction != null)
        //            {
        //                transaction.Commit();
        //                transaction = null;
        //            }
        //            conn.Close();
        //        }
        //    }
        //    catch (Exception)
        //    {
        //        try
        //        {
        //            if (transaction != null)
        //            {
        //                transaction.Rollback();
        //            }
        //        }
        //        catch { }
        //        throw;
        //    }
        //}

        

        public T ReadScalerDataFromDB<T>(string sql)
        {
            return ReadScalerDataFromDB<T>(new SQLQueryScaler<T>(sql));
        }

        public T ReadScalerDataFromDB<T>(SQLQueryScaler<T> query)
        {
            var task = ReadScalerDataFromDBAsync<T>(query);
            task.Wait();
            return task.Result;
        }

        public async Task<T> ReadScalerDataFromDBAsync<T>(string sql)
        {
            var query = new SQLQueryScaler<T>(sql);
            await RunQueryAsync(query);
            return query.ReturnValue;
        }
        
        public async Task<T> ReadScalerDataFromDBAsync<T>(SQLQueryScaler<T> query)
        {
            await RunQueryAsync(query);
            return query.ReturnValue;
        }

            //var conn = CreateConnection();
            //T returnResult = default(T);
            //using (conn)
            //{
            //    await conn.OpenAsync();
            //    var command = CreateCommand(query, conn);
            //    query.PreQueryProcess(query); 
            //    DumpSqlAndParamsToLog(query);
            //    var result = await command.ExecuteScalarAsync();
            //    query.RowCount = 1;
            //    query.PostQueryProcess(query);
            //    returnResult = ProcessScalerResult<T>(result);
            //    conn.Close();
            //}
            //return returnResult;
        
        

        //public async Task ReadDataFromDBAsync(IEnumerable<SQLQuery> queries)
        //{
        //    using (var conn = CreateConnection())
        //    {
        //        await conn.OpenAsync();
        //        bool abort = false;
        //        foreach (var groupNum in queries.Select(q => q.GroupNumber).Distinct().OrderBy(gn => gn))
        //        {
        //            var taskList = new Dictionary<SQLQuery, Task<DbDataReader>>();
        //            foreach (var query in queries.Where(q => q.GroupNumber == groupNum).OrderBy(q => q.OrderNumber))
        //            {
        //                query.PreQueryProcess(query);
        //                var command = CreateCommand(query, conn);
        //                DumpSqlAndParamsToLog(query);
        //                var task = command.ExecuteReaderAsync();
        //                taskList.Add(query, task);
        //            }
        //            await Task.WhenAll(taskList.Values);
        //            foreach (var query in taskList.Keys)
        //            {
        //                query.RowCount = 0;
        //                using (var reader = taskList[query].Result)
        //                {
        //                    while (await reader.ReadAsync())
        //                    {
        //                        query.RowCount++;
        //                        if (!query.ProcessRow(reader))
        //                        {
        //                            break;
        //                        }
        //                    }
        //                    reader.Close();
        //                }
        //                abort = abort || !query.PostQueryProcess(query);
        //            }
        //            if (abort)
        //            {
        //                break;
        //            }
        //        }
        //        conn.Close();
        //    }
        //}

        //public void ReadDataFromDB(IEnumerable<SQLQuery> queries)
        //{
        //    ReadDataFromDBAsync(SetQueryGroupsForSyncOperation(queries))
        //        .Wait();
        //}

        //public void ReadDataFromDB(SQLQuery query)
        //{
        //    ReadDataFromDB(new SQLQuery[] { query });
        //}

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

    public class QueryTask
    {
        public QueryTask(SQLQuery query, Task<int> nonQueryTask)
        {
            this.Query = query;
            this.NonQueryTask = nonQueryTask;
            this.ReaderTask = null;
            this.ScalerTask = null;
        }
        public QueryTask(SQLQuery query, Task<DbDataReader> readerTask)
        {
            this.Query = query;
            this.NonQueryTask = null;
            this.ReaderTask = readerTask;
            this.ScalerTask = null;
        }
        public QueryTask(SQLQuery query, Task<object> scalerTask)
        {
            this.Query = query;
            this.NonQueryTask = null;
            this.ReaderTask = null;
            this.ScalerTask = scalerTask;
        }
        public readonly SQLQuery Query;
        public readonly Task<int> NonQueryTask;
        public readonly Task<DbDataReader> ReaderTask;
        public readonly Task<object> ScalerTask;
        public Task Task
        {
            get
            {
                if (NonQueryTask != null) return NonQueryTask;
                if (ReaderTask != null) return ReaderTask;
                return ScalerTask;
            }
        }
    }

    public enum SQLQueryType
    {
        DataReader,
        Scaler,
        NonQuery
    }

    public class SQLQuery {
        public SQLQuery(string sql) {
            this.OriginalSQL = sql;
            this.ModifiedSQL = OriginalSQL;
            SQLQueryType = CSharpQueryHelper.SQLQueryType.DataReader;
            Parameters = new Dictionary<string, object>();
            InParameters = new Dictionary<string, List<object>>();
            GroupNumber = 0;
            OrderNumber = 0;
            CausedAbort = false;
            Executed = false;
        }
        public readonly Dictionary<string, object> Parameters;
        public readonly Dictionary<string, List<object>> InParameters;
        public readonly string OriginalSQL;
        public string ModifiedSQL { get; set; }
        public SQLQueryType SQLQueryType { get; set; }
        public int RowCount { get; set; }
        public int GroupNumber { get; set; }
        public int OrderNumber { get; set; }
        public bool Executed { get; set; }
        public bool CausedAbort { get; set; }
        private Action<SQLQuery> preQueryProcess;
        public virtual Action<SQLQuery> PreQueryProcess
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
        public virtual Func<DbDataReader, bool> ProcessRow {
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
        private Func<SQLQuery, bool> postQueryProcess;
        public virtual Func<SQLQuery, bool> PostQueryProcess
        {
            get
            {
                if (postQueryProcess == null)
                {
                    postQueryProcess = new Func<SQLQuery, bool>(query => { return true; });
                }
                return postQueryProcess;
            }
            set
            {
                postQueryProcess = value;
            }
        }
    }

    public interface ScalerQuery
    {
        void ProcessScalerResult(object result);
    }

    public class SQLQueryScaler<T> : SQLQuery, ScalerQuery
    {
        public SQLQueryScaler(string sql)
            : base(sql)
        {
            SQLQueryType = CSharpQueryHelper.SQLQueryType.Scaler;
            this.PostQueryProcess = new Func<SQLQuery, bool>(q =>
            {
                processScaler(this);
                return true;
            });
        }

        public T ReturnValue { get; set; }

        private Action<SQLQueryScaler<T>> processScaler;
        public Action<SQLQueryScaler<T>> ProcessScaler
        {
            get
            {
                if (processScaler == null)
                {
                    processScaler = new Action<SQLQueryScaler<T>>(q => { });
                }
                return processScaler;
            }
            set
            {
                processScaler = value;
            }
        }

        public void ProcessScalerResult(object result)
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
            this.ReturnValue = returnResult;
        }
    }
}
