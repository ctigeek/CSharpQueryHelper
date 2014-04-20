using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Moq.Protected;
using NUnit.Framework;

namespace CSharpQueryHelper.Example
{
    [TestFixture]
    public class StoreRepositoryTests
    {
        StoreRepository repo;
        Mock<IQueryHelper> queryHelper;

        [SetUp]
        public void Setup()
        {
            queryHelper = new Mock<IQueryHelper>();
            queryHelper.Setup(qh => qh.RunScalerQuery<int>(It.IsAny<SQLQueryScaler<int>>()))
                .Returns(123);

            repo = new StoreRepository(queryHelper.Object);
        }

        [Test]
        public void GetNumberOfOrdersWithStatusTest()
        {
            var val = repo.GetNumberOfOrdersWithStatus(OrderStatus.Ordered);
            Assert.AreEqual(123, val);
            queryHelper.Verify(qh => 
                    qh.RunScalerQuery<int>(It.Is<SQLQueryScaler<int>>(s => 
                        s.Parameters.Count == 1 && 
                        (OrderStatus)s.Parameters["OrderStatus"] == OrderStatus.Ordered)), 
                Times.Exactly(1));
        }

        [Test]
        public void GetOrderCountForAllStatusTest()
        {
            queryHelper.Setup(qh => qh.RunQuery(It.IsAny<SQLQuery>()))
                .Callback<SQLQuery>(q =>
                {
                    q.ProcessRow(GetStatusCountDataReader(OrderStatus.BackOrdered, 111));
                    q.ProcessRow(GetStatusCountDataReader(OrderStatus.Destroyed, 222));
                    q.ProcessRow(GetStatusCountDataReader(OrderStatus.Forgotten, 333));
                    q.ProcessRow(GetStatusCountDataReader(OrderStatus.Lost, 444));
                    q.ProcessRow(GetStatusCountDataReader(OrderStatus.Ordered, 555));
                });
            var orderCountDict = repo.GetOrderCountForAllStatus(new DateTime(2000, 1, 1), new DateTime(2000, 3, 31));
            Assert.AreEqual(111, orderCountDict[OrderStatus.BackOrdered]);
            Assert.AreEqual(222, orderCountDict[OrderStatus.Destroyed]);
            Assert.AreEqual(333, orderCountDict[OrderStatus.Forgotten]);
            Assert.AreEqual(444, orderCountDict[OrderStatus.Lost]);
            Assert.AreEqual(555, orderCountDict[OrderStatus.Ordered]);

        }


        private TestDataReader GetStatusCountDataReader(OrderStatus status, int count)
        {
            var returnValues = new Dictionary<string, object>();
            returnValues.Add("Status", (int)OrderStatus.BackOrdered);
            returnValues.Add("count", (int)123);
            return new TestDataReader(returnValues);
        }
    }

    public class TestDataReader : System.Data.Common.DbDataReader
    {
        private Dictionary<string, object> ReturnValues;
        public TestDataReader(Dictionary<string, object> returnValues)
        {
            this.ReturnValues = returnValues;
        }

        public override object this[string name]
        {
            get { return ReturnValues[name]; }
        }

        //unused methods that have to be overridden.

        public override bool Read()
        {
            throw new NotImplementedException();
        }
        public override bool NextResult()
        {
            throw new NotImplementedException();
        }
        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }
        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override int GetInt32(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override void Close()
        {
            throw new NotImplementedException();
        }
        public override int Depth
        {
            get { throw new NotImplementedException(); }
        }
        public override int FieldCount
        {
            get { throw new NotImplementedException(); }
        }
        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override System.Collections.IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public override System.Data.DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool HasRows
        {
            get { throw new NotImplementedException(); }
        }

        public override bool IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        public override int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        public override object this[int ordinal]
        {
            get { throw new NotImplementedException(); }
        }
    }
}
