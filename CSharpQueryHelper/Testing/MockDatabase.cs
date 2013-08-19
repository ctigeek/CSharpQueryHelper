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
        //public Mock<DbDataReader> DbDataReader { get; set; }
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

        public static Mock<MoqDbCommand> CreateDbCommand()
        {
            var dbCommand = new Mock<MoqDbCommand>(Parameters.Object);
            dbCommand.CallBase = true;
            dbCommand.SetupProperty(dbc => dbc.CommandText);
            return dbCommand;
        }
    }

    public abstract class MoqDbCommand : DbCommand
    {
        public MoqDbCommand(DbParameterCollection parameters)
        {
            this.parameters = parameters;
        }
        private DbParameterCollection parameters;
        
        protected override DbParameterCollection DbParameterCollection
        {
            get { return parameters; }
        }
    }

    
    //public class MockDatabaseTransaction : DbTransaction
    //{
    //    public override void Commit()
    //    {
    //    }

    //    private DbConnection dbConnection;
    //    protected override DbConnection DbConnection { get { return dbConnection; } }

    //    private IsolationLevel isolationLevel;
    //    public override IsolationLevel IsolationLevel { get { return isolationLevel; } }

    //    public override void Rollback()
    //    {
    //    }
    //}

    //public class MockDatabaseReader : DbDataReader
    //{
    //    public override void Close()
    //    {
    //    }

    //    private int depth;
    //    public override int Depth { get { return depth; } }
    //    private int fieldCount;
    //    public override int FieldCount { get { return fieldCount; } }
    //    public override bool GetBoolean(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override byte GetByte(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override char GetChar(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override string GetDataTypeName(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override DateTime GetDateTime(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override decimal GetDecimal(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override double GetDouble(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override System.Collections.IEnumerator GetEnumerator()
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override Type GetFieldType(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override float GetFloat(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override Guid GetGuid(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override short GetInt16(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override int GetInt32(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override long GetInt64(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override string GetName(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override int GetOrdinal(string name)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override DataTable GetSchemaTable()
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override string GetString(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override object GetValue(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override int GetValues(object[] values)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override bool HasRows { get { return true; } }

    //    public override bool IsClosed { get { return false; } }

    //    public override bool IsDBNull(int ordinal)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override bool NextResult()
    //    {
    //        //throw new NotImplementedException();
    //        return false;
    //    }

    //    public override bool Read()
    //    {
    //        //throw new NotImplementedException();
    //        return true;
    //    }

    //    public override int RecordsAffected { get { return 2; } }

    //    public override object this[string name]
    //    {
    //        get { throw new NotImplementedException(); }
    //    }

    //    public override object this[int ordinal]
    //    {
    //        get { throw new NotImplementedException(); }
    //    }
    //}

    //public class MockDatabaseParameter : DbParameter
    //{
    //    public override DbType DbType { get; set;}
    //    public override ParameterDirection Direction { get; set;}
    //    public override bool IsNullable { get; set; }
    //    public override string ParameterName { get; set; }
    //    public override void ResetDbType() 
    //    {
    //        throw new NotImplementedException();
    //    }
    //    public override int Size { get; set; }

    //    public override string SourceColumn { get; set; }
    //    public override bool SourceColumnNullMapping { get; set; }
    //    public override DataRowVersion SourceVersion { get; set; }
    //    public override object Value { get; set; }
    //}

    //public class MockDatabaseCommand : DbCommand
    //{

    //    public override void Cancel()
    //    {
    //    }

    //    public override string CommandText { get; set; }
    //    public override int CommandTimeout { get; set; }
    //    public override CommandType CommandType { get; set; }
    //    protected override DbParameter CreateDbParameter() 
    //    {
    //        return new MockDatabaseParameter();
    //    }

    //    protected override DbConnection DbConnection { get; set; }
    
    //    protected override DbParameterCollection DbParameterCollection
    //    {
    //        get { throw new NotImplementedException(); }
    //    }

    //    protected override DbTransaction DbTransaction { get; set; }
    //    public override bool DesignTimeVisible { get; set; }

    //    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    //    {
    //        //TODO:...
    //        return new MockDatabaseReader();
    //        //throw new NotImplementedException();
    //    }

    //    public override int ExecuteNonQuery()
    //    {
    //        //TODO:...
    //        return 1;
    //        //throw new NotImplementedException();
    //    }

    //    public override object ExecuteScalar()
    //    {
    //        return "hi";
    //    }

    //    public override void Prepare()
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override UpdateRowSource UpdatedRowSource { get; set; }
    //}

    //public class MockDatabaseConnection : DbConnection
    //{
    //    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    //    {
    //        return new MockDatabaseTransaction();
    //    }

    //    public override void ChangeDatabase(string databaseName)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public override void Close()
    //    {
    //    }

    //    public override string ConnectionString { get; set; }

    //    protected override DbCommand CreateDbCommand()
    //    {
    //        return new MockDatabaseCommand();
    //    }

    //    public override string DataSource
    //    {
    //        get { throw new NotImplementedException(); }
    //    }

    //    public override string Database
    //    {
    //        get { throw new NotImplementedException(); }
    //    }

    //    public override void Open()
    //    {
            
    //    }

    //    public override string ServerVersion
    //    {
    //        get { return string.Empty; }
    //    }

    //    public override ConnectionState State
    //    {
    //        get { return ConnectionState.Open; }
    //    }
    //}


}
