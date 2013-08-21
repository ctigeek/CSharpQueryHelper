using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;

namespace CSharpQueryHelper
{
    [TestFixture]
    public class Test
    {
        QueryHelper queryHelper;
        string connectionString = "connString";
        string provider = "SqlServerCe";
        Dictionary<string, object> dataRow;

        [TestFixtureSetUp]
        public void FixtureSetup()
        {
            
        }

        [SetUp]
        public void Setup()
        {
            MockDatabaseFactory.DbParameter = MockDatabaseFactory.CreateDbParameter();
            MockDatabaseFactory.Parameters = MockDatabaseFactory.CreateParameterCollection();
            MockDatabaseFactory.DbConnection = MockDatabaseFactory.CreateDbConnection();
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand();
            MockDatabaseFactory.DbTransaction = MockDatabaseFactory.CreateDbTransaction();

            queryHelper = new QueryHelper(connectionString, provider, new MockDatabaseFactory());
        }

        [Test]
        public void ReadScalerReturnsAString() 
        {
            string scalerReturn = "This is the return value.";
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(scalerReturn);
            var query = new SQLQueryWithParameters("select happyString from table;");
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.AreEqual(scalerReturn, returnValue);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerReturnsAStringWithParameters()
        {
            string scalerReturn = "This is the return value.";
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(scalerReturn);

            var query = new SQLQueryWithParameters("select happyString from table;");
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.AreEqual(scalerReturn, returnValue);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
        }

        [Test]
        public void ReadScalerIntReturnedAsString()
        {
            int scalerReturn = 555;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(scalerReturn);
            var query = new SQLQueryWithParameters("select happyString from table;");
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.AreEqual(scalerReturn.ToString(), returnValue);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
        }

        [Test]
        public void ReadScalerDbNullReturnsNullObject()
        {
            DBNull scalerReturn = DBNull.Value;
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(scalerReturn);
            var query = new SQLQueryWithParameters("select happyString from table;");
            var returnValue = queryHelper.ReadScalerDataFromDB<string>(query);

            Assert.IsNull(returnValue);
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Open(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.Verify(dbc => dbc.Close(), Times.Exactly(1));
        }

        [Test]
        public void ReadSingleRowNoParameters()
        {
            var dataContainer = new TestDataContainer();
            
            var dataReader = new Mock<MoqDataReader>(dataContainer.dataRow);
            dataReader.CallBase = true;
            dataReader.Setup(dr => dr.Close());

            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand(dataReader.Object);

            var query = new SQLQueryWithParameters("select happyString from table;", dataContainer.ProcessRow);
            queryHelper.ReadDataFromDB(query);

            dataContainer.AssertData();
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));

            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void ReadSingleRowWithParameters()
        {
            var dataContainer = new TestDataContainer();

            var dataReader = new Mock<MoqDataReader>(dataContainer.dataRow);
            dataReader.CallBase = true;
            dataReader.Setup(dr => dr.Close());

            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand(dataReader.Object);

            var query = new SQLQueryWithParameters("select happyString from table;", dataContainer.ProcessRow);
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            queryHelper.ReadDataFromDB(query);

            dataContainer.AssertData();
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));

            Assert.AreEqual(1, query.RowCount);
            dataReader.Verify(dr => dr.Read(), Times.Exactly(2));
        }

        [Test]
        public void NonQueryTestNoParametersNoIdentity()
        {
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand();
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery()).Returns(432);

            var query = new NonQueryWithParameters("insert into tableName values (1,2,3,4,5);");
            
            queryHelper.NonQueryToDB(query);

            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            Assert.AreEqual(432, query.RowCount);
        }

        [Test]
        public void NonQueryTestWithParametersNoIdentity()
        {
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand();
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery()).Returns(432);

            var query = new NonQueryWithParameters("insert into tableName values (1,2,3,4,5);");
            query.Parameters.Add("param1", "value1");
            query.Parameters.Add("param2", "value2");
            query.Parameters.Add("param3", 333);
            queryHelper.NonQueryToDB(query);

            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(3));
            Assert.AreEqual(432, query.RowCount);
        }

        [Test]
        public void NonQueryTestNoParametersWithIdentity()
        {
            decimal valueToReturn = 567;
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand();
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery()).Returns(432);
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteScalar()).Returns(valueToReturn); //this returns the identity value...

            int returnedPK = -1;
            var query = new NonQueryWithParameters("insert into tableName values (1,2,3,4,5);");
            query.SetPrimaryKey = new Action<int, NonQueryWithParameters>((pk, q) =>
            {
                returnedPK = pk;
            });

            queryHelper.NonQueryToDB(query);
            Assert.AreEqual((int)valueToReturn, returnedPK);
            MockDatabaseFactory.DbCommand.Verify(dbc => dbc.ExecuteScalar(), Times.Exactly(1));
            MockDatabaseFactory.DbConnection.VerifySet(dbc => dbc.ConnectionString = connectionString, Times.Exactly(1));
            MockDatabaseFactory.Parameters.Verify(p => p.Add(It.IsAny<DbParameter>()), Times.Exactly(0));
            Assert.AreEqual(432, query.RowCount);
        }

        [Test]
        public void NonQueryTransactionNoParametersNoIdentity()
        {
            int returnValue = 100;
            MockDatabaseFactory.DbCommand = MockDatabaseFactory.CreateDbCommand();
            MockDatabaseFactory.DbCommand.Setup(dbc => dbc.ExecuteNonQuery()).Returns(() => { returnValue++; return returnValue; });

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
