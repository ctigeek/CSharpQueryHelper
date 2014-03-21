using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Moq.Protected;
using NUnit.Framework;

namespace CSharpQueryHelper
{
    [TestFixture]
    public class Test
    {
        QueryHelper queryHelper;
        string connectionString = "connString";
        string provider = "SqlServerCe";
        string sqlString = "select someColumn from someTable;";
        string sqlInString = "select someColumn from someTable where someColumn in (@inParam);";
        string sqlInStringAfterProcessing = "select someColumn from someTable where someColumn in (@inParam0,@inParam1,@inParam2,@inParam3);";
        string logMessage;
        System.Diagnostics.TraceEventType logLevel;
        string scalerStringValue = "This is the return value.";

        [SetUp]
        public void Setup()
        {
            MockDatabaseFactory.DbParameter = MockDatabaseFactory.CreateDbParameter();
            MockDatabaseFactory.Parameters = MockDatabaseFactory.CreateParameterCollection();
            MockDatabaseFactory.DbConnection = MockDatabaseFactory.CreateDbConnection();
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand();
            MockDatabaseFactory.DbTransaction = MockDatabaseFactory.CreateDbTransaction();
            MockDatabaseFactory.SetScalerReturnValue(scalerStringValue);

            queryHelper = new QueryHelper(connectionString, provider, new MockDatabaseFactory());
            queryHelper.LogMessage = new Action<string, System.Diagnostics.TraceEventType>((message, level) =>
                {
                    logMessage = message;
                    logLevel = level;
                });
            queryHelper.DebugLoggingEnabled = true;
            logMessage = string.Empty;
            logLevel = System.Diagnostics.TraceEventType.Start;
        }

        [Test]
        public void ReadScalerReturnsAString() 
        {
            var query = new SQLQueryWithParameters(sqlString);
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringAsync()
        {
            var query = new SQLQueryWithParameters(sqlString);
            var task = queryHelper.ReadScalerDataFromDBAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringUsingSQLString()
        {
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(sqlString);

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringUsingSQLStringAsync()
        {
            var task = queryHelper.ReadScalerDataFromDBAsync<string>(sqlString);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringWithParameters()
        {
            var query = new SQLQueryWithParameters(sqlString);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringWithParametersAsync()
        {
            var query = new SQLQueryWithParameters(sqlString);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var task = queryHelper.ReadScalerDataFromDBAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerStringValue, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsString()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryWithParameters(sqlString);
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.AreEqual(scalerReturn.ToString(), returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsStringAsync()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryWithParameters(sqlString);
            var task = queryHelper.ReadScalerDataFromDBAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerReturn.ToString(), returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsInt()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryWithParameters(sqlString);
            var returnValue = queryHelper.ReadScalerDataFromDB<int>(query);

            Assert.AreEqual(scalerReturn, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerIntReturnedAsIntAsync()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryWithParameters(sqlString);
            var task = queryHelper.ReadScalerDataFromDBAsync<int>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.AreEqual(scalerReturn, returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerDbNullReturnsNullObject()
        {
            DBNull scalerReturn = DBNull.Value;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryWithParameters(sqlString);
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.IsNull(returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerDbNullReturnsNullObjectAsync()
        {
            DBNull scalerReturn = DBNull.Value;
            MockDatabaseFactory.SetScalerReturnValue(scalerReturn);
            var query = new SQLQueryWithParameters(sqlString);
            var task = queryHelper.ReadScalerDataFromDBAsync<string>(query);
            task.Wait();
            var returnValue = task.Result;

            Assert.IsNull(returnValue);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public void ReadSingleRowNoParameters()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQueryWithParameters(sqlString, dataContainer.ProcessRow);
            queryHelper.ReadDataFromDB(query);

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Protected()
                .Verify<DbDataReader>("ExecuteDbDataReader", Times.Exactly(1), It.IsAny<CommandBehavior>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowNoParametersAsync()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQueryWithParameters(sqlString, dataContainer.ProcessRow);
            var task = queryHelper.ReadDataFromDBAsync(query);
            task.Wait();

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Protected()
                .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithInParameters()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQueryWithParameters(sqlInString, dataContainer.ProcessRow);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var inList = new List<object>();
            inList.AddRange(new string[] { "val1", "val2", "val3", "val4" });
            query.InParameters.Add("inParam", inList);
            queryHelper.ReadDataFromDB(query);

            dataContainer.AssertData();
            VerifyLogging(sqlInStringAfterProcessing);

            Assert.AreEqual(sqlInStringAfterProcessing, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(7));
            MockDatabaseFactory.DbCommand.Protected()
                .Verify<DbDataReader>("ExecuteDbDataReader", Times.Exactly(1), It.IsAny<CommandBehavior>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithInParametersAsync()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQueryWithParameters(sqlInString, dataContainer.ProcessRow);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var inList = new List<object>();
            inList.AddRange(new string[] { "val1", "val2", "val3", "val4" });
            query.InParameters.Add("inParam", inList);
            var task = queryHelper.ReadDataFromDBAsync(query);
            task.Wait();

            dataContainer.AssertData();
            VerifyLogging(sqlInStringAfterProcessing);

            Assert.AreEqual(sqlInStringAfterProcessing, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(7));
            MockDatabaseFactory.DbCommand.Protected()
                            .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>()); 
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithParameters()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQueryWithParameters(sqlString, dataContainer.ProcessRow);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            queryHelper.ReadDataFromDB(query);

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Protected()
                .Verify<DbDataReader>("ExecuteDbDataReader", Times.Exactly(1), It.IsAny<CommandBehavior>());
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithParametersAsync()
        {
            var dataContainer = new TestDataContainer();
            var dataReader = MockDatabaseFactory.CreateDbDataReader(dataContainer);
            var query = new SQLQueryWithParameters(sqlString, dataContainer.ProcessRow);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var task = queryHelper.ReadDataFromDBAsync(query);
            task.Wait();

            dataContainer.AssertData();
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Protected()
                            .Verify<Task<DbDataReader>>("ExecuteDbDataReaderAsync", Times.Exactly(1), It.IsAny<CommandBehavior>(), It.IsAny<System.Threading.CancellationToken>());
            MockDatabaseFactory.DbCommand.Protected()
                .VerifyGet<DbParameterCollection>("DbParameterCollection", Times.Exactly(3));
            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void NonQueryTestNoParametersNoIdentity()
        {
            var query = new NonQueryWithParameters(sqlString);
            
            queryHelper.NonQueryToDB(query);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQuery(), Times.Exactly(1));
            Assert.AreEqual(543, query.RowCount);
        }

        [Test]
        public void NonQueryTestNoParametersNoIdentityAsync()
        {
            var query = new NonQueryWithParameters(sqlString);

            var task = queryHelper.NonQueryToDBAsync(query);
            task.Wait();

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersNoIdentity()
        {
            var query = new NonQueryWithParameters(sqlString);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            queryHelper.NonQueryToDB(query);

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQuery(), Times.Exactly(1));
            Assert.AreEqual(543, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersNoIdentityAsync()
        {
            var query = new NonQueryWithParameters(sqlString);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var task = queryHelper.NonQueryToDBAsync(query);
            task.Wait();

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersBuiltDynamicallyNoIdentity()
        {
            var query = new NonQueryWithParameters(sqlString);
            query.BuildParameters = new Action<SQLQuery>(q =>
            {
                q.Parameters.Add("param1", "value1");
                q.Parameters.Add("param2", "value2");
                q.Parameters.Add("param3", 333);
            });
            
            queryHelper.NonQueryToDB(query);
            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQuery(), Times.Exactly(1));
            Assert.AreEqual(543, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersBuiltDynamicallyNoIdentityAsync()
        {
            var query = new NonQueryWithParameters(sqlString);
            query.BuildParameters = new Action<SQLQuery>(q =>
            {
                q.Parameters.Add("param1", "value1");
                q.Parameters.Add("param2", "value2");
                q.Parameters.Add("param3", 333);
            });

            var task = queryHelper.NonQueryToDBAsync(query);
            task.Wait();

            VerifyLogging(sqlString);
            Assert.AreEqual(sqlString, MockDatabaseFactory.DbCommand.Object.CommandText);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTestNoParametersWithIdentity()
        {
            decimal valueToReturn = 567;
            MockDatabaseFactory.SetScalerReturnValue(valueToReturn);

            int returnedPK = -1;
            var query = new NonQueryWithParameters(sqlString);
            query.SetPrimaryKey = new Action<int, NonQueryWithParameters>((pk, q) =>
            {
                returnedPK = pk;
            });

            queryHelper.NonQueryToDB(query);
            Assert.AreEqual((int)valueToReturn, returnedPK);
            //VerifyLogging(sqlString);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            Assert.AreEqual(543, query.RowCount);
        }

        [Test]
        public void NonQueryTestNoParametersWithIdentityAsync()
        {
            decimal valueToReturn = 567;
            MockDatabaseFactory.SetScalerReturnValue(valueToReturn);

            int returnedPK = -1;
            var query = new NonQueryWithParameters(sqlString);
            query.SetPrimaryKey = new Action<int, NonQueryWithParameters>((pk, q) =>
            {
                returnedPK = pk;
            });

            var task = queryHelper.NonQueryToDBAsync(query);
            task.Wait();

            Assert.AreEqual((int)valueToReturn, returnedPK);
            //VerifyLogging(sqlString);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(345, query.RowCount);
        }

        [Test]
        public void NonQueryTransactionNoParametersNoIdentity()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery())
                .Returns(() => 
                { 
                    returnValue++; 
                    return returnValue; 
                });

            var queries = new List<NonQueryWithParameters>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(new NonQueryWithParameters("insert into sometable values (" + counter + ");") { Order=counter });
            }
            queryHelper.NonQueryToDBWithTransaction(queries);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries.FirstOrDefault(q => q.Order == counter).RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQuery(), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionNoParametersNoIdentityAsync()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                        .Returns(() =>   //you can't use retunrasync here becuse there's no way to increment the variable each time, not even with .callback.
                        { 
                            returnValue++; 
                            return Task.FromResult<int>(returnValue); 
                        });

            var queries = new List<NonQueryWithParameters>();
            for (int counter = 0; counter < 10; counter++)
            {
                queries.Add(new NonQueryWithParameters("insert into sometable values (" + counter + ");") { Order = counter });
            }
            var task = queryHelper.NonQueryToDBWithTransactionAsync(queries);
            task.Wait();

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries.FirstOrDefault(q => q.Order == counter).RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionWithParametersNoIdentity()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery())
                    .Returns(() => 
                    { 
                        returnValue++; 
                        return returnValue; 
                    });

            var queries = new List<NonQueryWithParameters>();
            for (int counter = 0; counter < 10; counter++)
            {
                var query = new NonQueryWithParameters("insert into sometable values (" + counter + ");") { Order = counter };
                query.Parameters.Add("param1", "value1");
                query.Parameters.Add("param2", "value2");
                query.Parameters.Add("param3", 333);
                queries.Add(query);
            }
            queryHelper.NonQueryToDBWithTransaction(queries);

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries.FirstOrDefault(q => q.Order == counter).RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQuery(), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(30));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionWithParametersNoIdentityAsync()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                       .Returns(() => 
                    {
                        returnValue++;
                        return Task.FromResult<int>(returnValue); 
                    });

            var queries = new List<NonQueryWithParameters>();
            for (int counter = 0; counter < 10; counter++)
            {
                var query = new NonQueryWithParameters("insert into sometable values (" + counter + ");") { Order = counter };
                query.Parameters.Add("param1", "value1");
                query.Parameters.Add("param2", "value2");
                query.Parameters.Add("param3", 333);
                queries.Add(query);
            }
            var task = queryHelper.NonQueryToDBWithTransactionAsync(queries);
            task.Wait();

            for (int counter = 0; counter < 10; counter++)
            {
                Assert.AreEqual(101 + counter, queries.FirstOrDefault(q => q.Order == counter).RowCount);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(30));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionNoParametersWithIdentity()
        {
            int returnValue = 100;
            decimal identityValue = 200;
            Dictionary<int, int> primarykeysSet = new Dictionary<int, int>();
            
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery()).Returns(() => { returnValue++; return returnValue; });
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(() => { identityValue += 1; return identityValue; }); //this returns the identity value...

            var queries = new List<NonQueryWithParameters>();
            for (int counter = 0; counter < 10; counter++)
            {
                var query = new NonQueryWithParameters("insert into sometable values (" + counter + ");") { Order = counter };
                query.SetPrimaryKey = (pk, q) => { primarykeysSet.Add(q.Order, pk); };
                queries.Add(query);
            }
            queryHelper.NonQueryToDBWithTransaction(queries);

            for (int counter = 0; counter < 10; counter++)
            {
                var query = queries.FirstOrDefault(q => q.Order == counter);
                Assert.AreEqual(101 + counter, query.RowCount);
                Assert.AreEqual(201 + counter, primarykeysSet[query.Order]);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQuery(), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test]
        public void NonQueryTransactionNoParametersWithIdentityAsync()
        {
            int returnValue = 100;
            int identityValue = 200;
            Dictionary<int, int> primarykeysSet = new Dictionary<int, int>();

            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                .Returns(() => { returnValue++; return Task.FromResult<int>(returnValue); });

            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalarAsync(It.IsAny<System.Threading.CancellationToken>()))
                   .Returns(() => { identityValue++; return Task.FromResult<object>(identityValue); });

            var queries = new List<NonQueryWithParameters>();
            for (int counter = 0; counter < 10; counter++)
            {
                var query = new NonQueryWithParameters("insert into sometable values (" + counter + ");") { Order = counter };
                query.SetPrimaryKey = (pk, q) => { primarykeysSet.Add(q.Order, pk); };
                queries.Add(query);
            }

            var task = queryHelper.NonQueryToDBWithTransactionAsync(queries);
            task.Wait();

            for (int counter = 0; counter < 10; counter++)
            {
                var query = queries.FirstOrDefault(q => q.Order == counter);
                Assert.AreEqual(101 + counter, query.RowCount);
                Assert.AreEqual(201 + counter, primarykeysSet[query.Order]);
            }
            MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(10));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(1));
            MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(0));
        }

        [Test,ExpectedException("System.ApplicationException", UserMessage="blah blah")]
        public void NonQueryTransactionNoParametersNoIdentityRollbackWhenException()
        {
            try
            {
                int returnValue = 100;
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery())
                    .Returns(() =>
                    {
                        returnValue++;
                        if (returnValue == 105) throw new ApplicationException("blah blah");
                        return returnValue;
                    });

                var queries = new List<NonQueryWithParameters>();
                for (int counter = 0; counter < 10; counter++)
                {
                    var query = new NonQueryWithParameters("insert into sometable values (" + counter + ");");
                    query.Order = counter;
                    queries.Add(query);
                }
                queryHelper.NonQueryToDBWithTransaction(queries);
            }
            finally
            {
                MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
                MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQuery(), Times.Exactly(5));
                MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
                MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(1));
            }
        }

        [Test, ExpectedException("System.ApplicationException", UserMessage = "blah blah")]
        public void NonQueryTransactionNoParametersNoIdentityRollbackWhenExceptionAsync()
        {
            try
            {
                int returnValue = 100;
                MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()))
                       .Returns(() =>
                       {
                           returnValue++;
                           if (returnValue == 105) throw new ApplicationException("blah blah");
                           return Task.FromResult<int>(returnValue);
                       });

                var queries = new List<NonQueryWithParameters>();
                for (int counter = 0; counter < 10; counter++)
                {
                    var query = new NonQueryWithParameters("insert into sometable values (" + counter + ");");
                    query.Order = counter;
                    queries.Add(query);
                }
                var task = queryHelper.NonQueryToDBWithTransactionAsync(queries);
                task.Wait();
            }
            catch (System.AggregateException ex)
            {
                throw ex.InnerExceptions[0];
            }
            finally
            {
                MockDatabaseFactory.DbCommand.VerifySet(dbc => dbc.Transaction = MockDatabaseFactory.DbTransaction.Object);
                MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteNonQueryAsync(It.IsAny<System.Threading.CancellationToken>()), Times.Exactly(5));
                MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
                MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Commit(), Times.Exactly(0));
                MockDatabaseFactory.DbTransaction.Verify(dbt => dbt.Rollback(), Times.Exactly(1));
            }
        }

        private void VerifyLogging(string sql)
        {
            Assert.True(this.logMessage.Contains(sql));
            Assert.AreEqual(System.Diagnostics.TraceEventType.Verbose, this.logLevel);
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
