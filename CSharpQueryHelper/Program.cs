using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpQueryHelper
{
    class Program
    {
        static void Main(string[] args)
        {

            try
            {
                var sql = "insert into table2 (somestring, someval) values (@somestring, @someval);";
                var query1 = new SQLQuery(sql);
                query1.Parameters.Add("somestring", "blahbla1");
                query1.Parameters.Add("someval", 123);

                var query2 = new SQLQuery(sql);
                query2.Parameters.Add("somestring", "blahbla2");
                query2.Parameters.Add("someval", 321);

                
                //query.ProcessRow = new Func<System.Data.Common.DbDataReader, bool>(dr =>
                //{
                //    Console.WriteLine(dr[1]);
                //    return true;
                //});

                var qh = new QueryHelper("sandbox");

                qh.NonQueryToDBWithTransaction(new[] { query1, query2 });

                //    .ReadScalerDataFromDB<int>("select top 1 pk from table1;");

                //Console.WriteLine(val);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            Console.WriteLine("press any key");
            Console.ReadLine();
        }
    }

    public class ParentObject
    {
        public string Name { get; set; }
        public int SomeProp { get; set; }
        public List<ChildObject> ChildObjects { get; set; }
    }

    public class ChildObject
    {
        public string Name { get; set; }
        public DateTime Created { get; set; }
        public int SomeProp { get; set; }
    }

    public static class Extensions
    {
        public static SQLQuery GetInsertQuery(this ChildObject childObject, Action<SQLQuery> preQuery)
        {
            string sql = "insert into childTable (ParentPK, Name, Created, SomeProp) values (@parentpk,@name,@created,@someprop);";
            var query = new SQLQuery(sql);
            query.Parameters.Add("name", childObject.Name);
            query.Parameters.Add("created", childObject.Created);
            query.Parameters.Add("someprop", childObject.SomeProp);
            query.PreQueryProcess = preQuery;
            return query;
        }
        public static SQLQuery GetInsertQuery(this ParentObject parentObject)
        {
            string sql = "insert into parentTable (Name, SomeProp) OUTPUT Inserted.pk values (@name, @someprop);";
            var query = new SQLQuery(sql);
            query.Parameters.Add("name", parentObject.Name);
            query.Parameters.Add("someprop", parentObject.SomeProp);

            return query;
        }        
    }


}
