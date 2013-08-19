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

    }
}
