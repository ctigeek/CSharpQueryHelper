using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;

namespace CSharpQueryHelper
{
    public class MockDatabaseFactory : DbProviderFactory
    {
        public static Mock<DbTransaction> DbTransaction { get; set; }
        public static Mock<MoqDbConnection> DbConnection { get; set; }
        public static Mock<MoqDbCommand> DbCommand { get; set; }
        public static Mock<DbParameter> DbParameter { get; set; }
        public static Mock<DbParameterCollection> Parameters { get; set; }

        public override DbCommand CreateCommand()
        {
            return (DbCommand)DbCommand.Object;
        }
        public override DbConnection CreateConnection()
        {
            return DbConnection.Object;
        }
        public override DbParameter CreateParameter()
        {
            return DbParameter.Object;
        }

        public static Mock<DbTransaction> CreateDbTransaction()
        {
            var dbTransaction = new Mock<DbTransaction>();
            dbTransaction.CallBase = true;
            dbTransaction.Setup(dbt => dbt.Commit());
            dbTransaction.Setup(dbt => dbt.Rollback());
            return dbTransaction;
        }
        public static Mock<MoqDbConnection> CreateDbConnection()
        {
            var dbConnection = new Mock<MoqDbConnection>();
            dbConnection.CallBase = true;
            dbConnection.SetupProperty(dbc => dbc.ConnectionString);
            dbConnection.Setup(dbc => dbc.Open());
            dbConnection.Setup(dbc => dbc.Close());

            return dbConnection;
        }
        public static Mock<DbParameter> CreateDbParameter()
        {
            var dbParameter = new Mock<DbParameter>();
            dbParameter.SetupProperty(dbc => dbc.ParameterName);
            dbParameter.SetupProperty(dbc => dbc.Value);
            return dbParameter;
        }
        public static Mock<DbParameterCollection> CreateParameterCollection()
        {
            var parameters = new Mock<DbParameterCollection>();
            parameters.Setup(p => p.Add(It.IsAny<DbParameter>()));
            return parameters;
        }
        public static Mock<MoqDbCommand> CreateDbCommand(DbDataReader dataReader = null)
        {
            var dbCommand = (dataReader == null) ?
                new Mock<MoqDbCommand>(Parameters.Object) :
                new Mock<MoqDbCommand>(Parameters.Object, dataReader);
            dbCommand.CallBase = true;
            dbCommand.SetupProperty(dbc => dbc.CommandText);
            return dbCommand;
        }
    }

    public abstract class MoqDbConnection : DbConnection
    {
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return MockDatabaseFactory.DbTransaction.Object;
        }
    }

    public abstract class MoqDbCommand : DbCommand
    {
        public DbDataReader DataReader { get; private set; }

        public MoqDbCommand(DbParameterCollection parameters)
        {
            this.parameters = parameters;
        }
        public MoqDbCommand(DbParameterCollection parameters, DbDataReader dataReader) :
            this(parameters)
        {
            this.DataReader = dataReader;
        }

        private DbParameterCollection parameters;
        
        protected override DbParameterCollection DbParameterCollection
        {
            get { return parameters; }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return DataReader;
        }
    }

    public abstract class MoqDataReader : DbDataReader
    {
        public virtual Dictionary<string, object> dataRow { get; private set; }
        bool lineRead = false;

        public MoqDataReader(Dictionary<string, object> dataRow)
        {
            this.dataRow = dataRow;
        }

        public override bool Read()
        {
            if (!lineRead)
            {
                lineRead = true;
                return true;
            }
            return false;
        }

        public override object this[string name]
        {
            get {
                if (dataRow.ContainsKey(name))
                {
                    return dataRow[name];
                }
                return null;
            }
        }

        public override object this[int ordinal]
        {
            get { throw new NotImplementedException(); }
        }
    }

}
