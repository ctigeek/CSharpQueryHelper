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
        public static Mock<DbConnection> DbConnection { get; set; }
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

        public static Mock<DbConnection> CreateDbConnection()
        {
            var dbConnection = new Mock<DbConnection>();
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

        //public override void Close()
        //{
        //}

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

        public override Guid GetGuid(int ordinal)
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

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException("GetValue....");
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool HasRows
        {
            get { throw new NotImplementedException("Has rows..."); }
        }

        public override bool IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            if (!lineRead)
            {
                lineRead = true;
                return true;
            }
            return false;
            //throw new NotImplementedException();
        }

        public override int RecordsAffected
        {
            get { throw new NotImplementedException(); }
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
