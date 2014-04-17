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
        Task RunQueryAsync(SQLQuery query);
        Task RunQueryAsync(IEnumerable<SQLQuery> queries, bool withTransaction = false);
        void RunNonQuery(string sql);
        T RunScalerQuery<T>(string sql);
        Task<T> RunScalerQueryAsync<T>(string sql);
        T RunScalerQuery<T>(SQLQueryScaler<T> query);
        Task<T> RunScalerQueryAsync<T>(SQLQueryScaler<T> query);
    }

    public interface ITransactable
    {
        bool TransactionOpen { get; }
        void StartTransaction();
        void CommitTransaction();
        void RollbackTransaction();
    }

    public class QueryHelper : IQueryHelper, ITransactable, IDisposable
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
            externalTransactionInProgress = false;
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
            externalTransactionInProgress = false;
        }

        public void Dispose()
        {
            if (externalTransactionInProgress)
            {
                RollbackTransaction();
            }
        }

        #region external transactions
        private DbConnection persistentConnection;
        private DbTransaction persistentTransaction;

        private bool externalTransactionInProgress;
        public bool TransactionOpen
        {
            get
            {
                return externalTransactionInProgress;
            }
        }
        public async void StartTransaction()
        {
            if (externalTransactionInProgress)
            {
                throw new InvalidOperationException("There is already a transaction in progress.");
            }

            externalTransactionInProgress = true;
            var conn = await GetOpenConnection();
            var tran = GetTransaction(conn);
        }
        public void CommitTransaction()
        {
            if (!externalTransactionInProgress)
            {
                throw new InvalidOperationException("There is not a transaction in progress.");
            }
            externalTransactionInProgress = false;
            CommitDbTransaction(persistentTransaction);
            CloseConnection(persistentConnection);
        }
        public void RollbackTransaction()
        {
            if (!externalTransactionInProgress)
            {
                throw new InvalidOperationException("There is not a transaction in progress.");
            }
            externalTransactionInProgress = false;
            RollbackDbTransaction(persistentTransaction);
            CloseConnection(persistentConnection);
        }
        #endregion
        public void RunNonQuery(string sql)
        {
            RunQuery(new [] { new SQLQuery(sql, SQLQueryType.NonQuery) }, externalTransactionInProgress);
        }
        public void RunQuery(SQLQuery query)
        {
            RunQuery(new [] { query }, externalTransactionInProgress);
        }
        public void RunQuery(IEnumerable<SQLQuery> queries, bool withTransaction = false)
        {
            RunQueryAsync(SetQueryGroupsForSyncOperation(queries), withTransaction)
                .Wait();
        }

        public async Task RunQueryAsync(SQLQuery query)
        {
            await RunQueryAsync(new[] { query }, externalTransactionInProgress);
        }
        public async Task RunQueryAsync(IEnumerable<SQLQuery> queries, bool withTransaction = false)
        {
            DbTransaction transaction = null;
            DbConnection connection = null;
            try
            {
                if (externalTransactionInProgress && !withTransaction)
                {
                    throw new ArgumentException("If an external transaction is open, then withTransaction parameter must be set to `true`.");
                }
                connection = await GetOpenConnection();
                if (withTransaction || externalTransactionInProgress)
                {
                    transaction = GetTransaction(connection);
                }
                foreach (var groupNum in queries.Select(q => q.GroupNumber).Distinct().OrderBy(gn => gn))
                {
                    var taskList = new List<QueryTask>();
                    foreach (var query in queries.Where(q => q.GroupNumber == groupNum).OrderBy(q => q.OrderNumber))
                    {
                        var queryTask = ExecuteQuery(query, connection, transaction);
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
                        queryTask.Query.CausedAbort = !queryTask.Query.PostQueryProcess(queryTask.Query);
                    }
                    if (taskList.Exists(qt => qt.Query.CausedAbort == true))
                    {
                        break;
                    }
                }
                CommitDbTransaction(transaction);
            }
            catch (Exception)
            {
                try
                {
                    RollbackDbTransaction(transaction);
                }
                catch { }
                throw;
            }
            finally
            {
                CloseConnection(connection);
            }
        }

        private void RollbackDbTransaction(DbTransaction transaction)
        {
            if (!externalTransactionInProgress && transaction != null)
            {
                persistentTransaction = null;
                transaction.Rollback();
            }
        }

        private void CommitDbTransaction(DbTransaction transaction)
        {
            if (!externalTransactionInProgress && transaction != null)
            {
                persistentTransaction = null;
                transaction.Commit();
            }
        }

        private DbTransaction GetTransaction(DbConnection connection)
        {
            if (externalTransactionInProgress && persistentTransaction != null)
            {
                return persistentTransaction;
            }
            else
            {
                var transaction = connection.BeginTransaction();
                if (externalTransactionInProgress)
                {
                    persistentTransaction = transaction;
                }
                return transaction;
            }
        }

        private void CloseConnection(DbConnection connection)
        {
            if (!externalTransactionInProgress)
            {
                persistentConnection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        private async Task<DbConnection> GetOpenConnection()
        {
            if (externalTransactionInProgress && persistentConnection != null)
            {
                return await Task.FromResult<DbConnection>(persistentConnection);
            }
            else
            {
                var conn = CreateConnection();
                await conn.OpenAsync();
                if (externalTransactionInProgress)
                {
                    persistentConnection = conn;
                }
                return conn;
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
                if (queryTask.Query is IScalerQuery)
                {
                    IScalerQuery scalerQuery = (IScalerQuery)queryTask.Query;
                    scalerQuery.ProcessScalerResult(result);
                }
            }
        }

        private QueryTask ExecuteQuery(SQLQuery query, DbConnection connection, DbTransaction transaction)
        {
            query.PreQueryProcess(query);
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

        public T RunScalerQuery<T>(string sql)
        {
            return RunScalerQuery<T>(new SQLQueryScaler<T>(sql));
        }

        public T RunScalerQuery<T>(SQLQueryScaler<T> query)
        {
            var task = RunScalerQueryAsync<T>(query);
            task.Wait();
            return task.Result;
        }

        public async Task<T> RunScalerQueryAsync<T>(string sql)
        {
            var query = new SQLQueryScaler<T>(sql);
            await RunQueryAsync(query);
            return query.ReturnValue;
        }
        
        public async Task<T> RunScalerQueryAsync<T>(SQLQueryScaler<T> query)
        {
            await RunQueryAsync(query);
            return query.ReturnValue;
        }

        private List<SQLQuery> SetQueryGroupsForSyncOperation(IEnumerable<SQLQuery> queries)
        {
            int groupNum = 1;
            var queryList = queries.ToList();
            foreach (var query in queryList.OrderBy(q=>q.GroupNumber).ThenBy(q=>q.OrderNumber))
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
        public SQLQuery(string sql, SQLQueryType queryType) {
            this.OriginalSQL = sql;
            this.ModifiedSQL = OriginalSQL;
            SQLQueryType = queryType;
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
        public readonly SQLQueryType SQLQueryType;
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

    public interface IScalerQuery
    {
        void ProcessScalerResult(object result);
    }

    public class SQLQueryScaler<T> : SQLQuery, IScalerQuery
    {
        public SQLQueryScaler(string sql)
            : base(sql, SQLQueryType.Scaler)
        {
        }

        public T ReturnValue { get; set; }

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
