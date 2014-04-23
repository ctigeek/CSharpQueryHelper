using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Moq.Protected;
using System.Transactions;
using NUnit.Framework;

namespace CSharpQueryHelper
{
    public class MockDatabaseFactory : DbProviderFactory
    {
        public static Mock<DbTransaction> DbTransaction { get; set; }
        public static Mock<MoqDbConnection> DbConnection { get; set; }
        public static Mock<DbCommand> DbCommand { get; set; }
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

        public static Mock<MoqDataReader> CreateDbDataReader(TestDataContainer dataContainer)
        {
            var dataReader = new Mock<MoqDataReader>(dataContainer.dataRow);
            dataReader.CallBase = true;
            dataReader.Setup(dr => dr.Close());
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand(dataReader.Object);
            return dataReader;
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
        public static void SetScalerReturnValue(object value)
        {
            if (MockDatabaseFactory.DbCommand != null)
            {
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(value);
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()))
                   .ReturnsAsync(value);
            }
        }
        public static Mock<DbCommand> CreateDbCommand(DbDataReader dataReader = null)
        {
            var dbCommand = new Mock<DbCommand>();
            //http://blogs.clariusconsulting.net/kzu/mocking-protected-members-with-moq/
            if (dataReader != null)
            {
                dbCommand.Protected()
                    .Setup<DbDataReader>("ExecuteDbDataReader", It.IsAny<CommandBehavior>())
                    .Returns(dataReader);
                dbCommand.Protected()
                    .Setup<Task<DbDataReader>>("ExecuteDbDataReaderAsync", It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>())
                    .Returns(Task.FromResult<DbDataReader>(dataReader));
            }
            dbCommand.Setup(dbc => dbc.ExecuteNonQuery())
                        .Returns(543);
            dbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                        .ReturnsAsync(345);

            dbCommand.Protected()
                .SetupGet<DbParameterCollection>("DbParameterCollection")
                .Returns(Parameters.Object);

            dbCommand.CallBase = true;
            dbCommand.SetupProperty(dbc => dbc.CommandText);
            return dbCommand;
        }
    }

    public abstract class MoqDbConnection : DbConnection, IEnlistmentNotification
    {
        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        {
            return MockDatabaseFactory.DbTransaction.Object;
        }

        private Transaction transaction;
        public override void EnlistTransaction(Transaction transaction)
        {
            RollbackCallCount = 0;
            CommitCallCount = 0;
            InDoubtCallCount = 0;
            PrepareCallCount = 0;

            this.transaction = transaction;
            this.transaction.EnlistVolatile(this, EnlistmentOptions.None);
        }

        public int CommitCallCount { get; private set; }
        public void Commit(Enlistment enlistment)
        {
            CommitCallCount++;
            enlistment.Done();
        }
        public int InDoubtCallCount { get; private set; }
        public void InDoubt(Enlistment enlistment)
        {
            InDoubtCallCount++;
            enlistment.Done();
        }
        public int PrepareCallCount { get; private set; }
        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            PrepareCallCount++;
            preparingEnlistment.Prepared();
        }
        public int RollbackCallCount { get; private set; }
        public void Rollback(Enlistment enlistment)
        {
            RollbackCallCount++;
            enlistment.Done();
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
            get
            {
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

    public class TestDataContainer
    {
        public Dictionary<string, object> dataRow;

        public TestDataContainer()
        {
            dataRow = new Dictionary<string, object>();
            dataRow.Add("column1", 1);
            dataRow.Add("column2", "3");
            dataRow.Add("column3", DateTime.Parse("1/1/2000"));
        }

        public int column1 = 0;
        public string column2 = string.Empty;
        public DateTime column3 = DateTime.MinValue;

        public void AssertData()
        {
            Assert.AreEqual(1, column1);
            Assert.AreEqual("3", column2);
            Assert.AreEqual(DateTime.Parse("1/1/2000"), column3);
        }

        public Func<DbDataReader, bool> ProcessRow
        {
            get
            {
                return dr =>
                {
                    column1 = (int)dr["column1"];
                    column2 = (string)dr["column2"];
                    column3 = (DateTime)dr["column3"];
                    return true;
                };
            }
        }
    }
}
