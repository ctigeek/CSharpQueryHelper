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

                var parent = new ParentObject();
                parent.Name = "Mom";
                parent.SomeProp = 123;
                parent.ChildObjects = new List<ChildObject>();

                for (int i = 1; i < 20; i++)
                {
                    var child = new ChildObject();
                    child.Name = "BioUnit number " + i.ToString();
                    child.SomeProp = i;
                    parent.ChildObjects.Add(child);
                }

                var queryList = new List<SQLQuery>();

                var parentQuery = parent.GetInsertQuery();
                parentQuery.GroupNumber = 1;
                parentQuery.OrderNumber = 1;
                int parentKey = 0;
                parentQuery.PostQueryProcess = q =>
                {
                    parentKey = parentQuery.ReturnValue;
                    return true;
                };
                queryList.Add(parentQuery);
                int orderNumber =1;
                foreach (var child in parent.ChildObjects)
                {
                    var childQuery = child.GetInsertQuery(q =>
                    {
                        q.Parameters.Add("parentpk", parentKey);
                    });
                    childQuery.GroupNumber = 2;
                    childQuery.OrderNumber = orderNumber;
                    orderNumber++;
                    queryList.Add(childQuery);
                }
                
                var qh = new QueryHelper("sandbox");
                qh.RunQuery(queryList, true);



                
                //var sql = "select someString from table1 where pk=9;";

                //var scalerQuery = new SQLQueryScaler<string>(sql);
                
                
                
                
                
                //var query = new SQLQuery(sql, SQLQueryType.DataReader);

                //query.ProcessRow = new Func<System.Data.Common.DbDataReader, bool>(dr =>
                //{
                //    Console.WriteLine("pk={0}  someString={1}  someFK={2}", dr[0], dr[1], dr[2]);
                //    return true;
                //});
                

                //var someString = qh.RunScalerQuery<string>(sql);
                //Console.WriteLine(someString);

                //qh.RunQuery(query);
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
        public int SomeProp { get; set; }
    }

    public static class Extensions
    {
        public static SQLQuery GetInsertQuery(this ChildObject childObject, Action<SQLQuery> preQuery)
        {
            string sql = "insert into Child (parent_pk, Name, SomeProp) values (@parentpk,@name,@someprop);";
            var query = new SQLQuery(sql, SQLQueryType.NonQuery);
            query.Parameters.Add("name", childObject.Name);
            query.Parameters.Add("someprop", childObject.SomeProp);
            query.PreQueryProcess = preQuery;
            return query;
        }
        public static SQLQueryScaler<int> GetInsertQuery(this ParentObject parentObject)
        {
            string sql = "insert into Parent (Name, SomeProp) OUTPUT Inserted.pk values (@name, @someprop);";
            var query = new SQLQueryScaler<int>(sql);
            query.Parameters.Add("name", parentObject.Name);
            query.Parameters.Add("someprop", parentObject.SomeProp);

            return query;
        }        
    }


}
