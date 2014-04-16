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

                parent.SaveParent();

                var parents = Extensions.LoadParents();

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
        public static List<ParentObject> LoadParents()
        {
            var parentDict = new Dictionary<int, ParentObject>();
            var sql = @"select Parent.pk as ParentPk,
                                Parent.Name as ParentName,
                                Parent.SomeProp as ParentSomeProp,
                                Child.pk as ChildPk,
                                Child.Name as ChildName,
                                Child.SomeProp as ChildSomeProp
                            from Parent inner join Child on (Parent.pk = Child.parent_pk)";
            var query = new SQLQuery(sql, SQLQueryType.DataReader);
            query.ProcessRow = dr =>
            {
                //add parent
                int parentpk = (int)dr["ParentPk"];
                if (!parentDict.ContainsKey(parentpk))
                {
                    var newParentObject = new ParentObject()
                    {
                        ChildObjects = new List<ChildObject>(),
                        Name = (string)dr["ParentName"],
                        SomeProp = (int)dr["ParentSomeProp"]
                    };
                    parentDict.Add(parentpk, newParentObject);
                }
                var parentObject = parentDict[parentpk];
                
                //add child
                var newChildObject = new ChildObject()
                {
                    Name = (string)dr["ChildName"],
                    SomeProp = (int)dr["ChildSomeProp"]
                };
                parentObject.ChildObjects.Add(newChildObject);

                return true;
            };

            var qh = new QueryHelper("sandbox");
            qh.RunQuery(query);

            return parentDict.Values.ToList();
        }
        public static void SaveParent(this ParentObject parent)
        {
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
            int orderNumber = 1;
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
        }

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

/*
CREATE TABLE [dbo].[Child](
	[pk] [int] IDENTITY(1,1) NOT NULL,
	[parent_pk] [int] NOT NULL,
	[Name] [nvarchar](50) NOT NULL,
	[SomeProp] [int] NOT NULL,
 CONSTRAINT [PK_Chile] PRIMARY KEY CLUSTERED 
 (	[pk] ASC)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
 ) 

 CREATE TABLE [dbo].[Parent](
	[pk] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](50) NOT NULL,
	[SomeProp] [int] NOT NULL,
 CONSTRAINT [PK_Parent] PRIMARY KEY CLUSTERED 
 ([pk] ASC)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
 )
*/

}
