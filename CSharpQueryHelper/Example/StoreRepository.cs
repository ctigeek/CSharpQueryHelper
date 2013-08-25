using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;

namespace CSharpQueryHelper.Example
{
    public class StoreRepository
    {
        public const string SQLCountOrderOfStatus = "select count(*) as count from Orders where Status = @OrderStatus;";
        public const string SQLCountOrderAllStatusByDate = "select count(*) as count, Status from Orders where OrderDateTime >= @Stardate and OrderDateTime <= @Enddate group by Status;";
        public const string SQLSelectSingleCustomer = "select * from Customers where PK=@CustomerId;";
        public const string SQLUpdateSingleCustomer = "update Customers set Name=@Name, EmailAddress=@EmailAddress, Password=@Password where PK=@CustomerId;";
        public const string SQLCreateCustomer = "insert into Customers (Name,EmailAddress,Password) values (@Name, @EmailAddress, @Password);";
        public const string SQLInsertOrder = "insert into Orders (OrderDatetime,CustomerPK,Status) values (@Datetime,@CustomerPK,@Status);";
        public const string SQLUpdateOrder = "";
        public const string SQLInsertOrderItem = "insert into OrderItems (Order_PK,Quantity,Inventory_PK,PricePer) values (@Order_PK,@Quantity,@Inventory_PK,@PricePer);";
        public const string SQLUpdateOrderItem = "";

        private IQueryHelper queryHelper;
        public StoreRepository(IQueryHelper qh)
        {
            this.queryHelper = qh;
        }

        
        #region Orders and OrderItems
        public int GetNumberOfOrdersWithStatus(OrderStatus orderStatus)
        {
            var query = new SQLQueryWithParameters(SQLCountOrderOfStatus);
            query.Parameters.Add("OrderStatus", (int)orderStatus);
            var count = queryHelper.ReadScalerDataFromDB<int>(query);
            return count;
        }
        public Dictionary<OrderStatus, int> GetOrderCountForAllStatus(DateTime startDate, DateTime endDate)
        {
            var returnDict = new Dictionary<OrderStatus, int>();
            var processRow = new Func<DbDataReader, bool>(dr =>
            {
                returnDict.Add((OrderStatus)(int)dr["Status"], (int)dr["count"]);
                return true;
            });
            var query = new SQLQueryWithParameters(SQLCountOrderAllStatusByDate, processRow);
            query.Parameters.Add("StartDate", startDate);
            query.Parameters.Add("EndDate", endDate);
            queryHelper.ReadDataFromDB(query);
            return returnDict;
        }
        public Order AddNewOrderAndItems(Order order)
        {
            int orderCounter = 0;
            var queries = new List<NonQueryWithParameters>();
            var query = GetOrderQuery(order);
            query.Order = orderCounter;
            queries.Add(query);
            foreach (OrderItem oi in order.OrderItems)
            {
                orderCounter++;
                var oiQuery = GetOrderItemQuery(oi);
                oiQuery.Order = orderCounter;
                queries.Add(oiQuery);
            }
            queryHelper.NonQueryToDBWithTransaction(queries);

            return order;
        }

        private NonQueryWithParameters GetOrderItemQuery(OrderItem orderItem)
        {
            NonQueryWithParameters query = (orderItem.PK > 0) ?
                new NonQueryWithParameters(SQLUpdateOrderItem) :
                new NonQueryWithParameters(SQLInsertOrderItem);
            query.Parameters.Add("Order_PK", orderItem.Order.PK);
            query.Parameters.Add("Quantity", orderItem.Quantity);
            query.Parameters.Add("Inventory_PK", orderItem.InventoryID);
            query.Parameters.Add("PricePer", orderItem.Price);
            if (orderItem.PK > 0)
            {
                query.Parameters.Add("PK", orderItem.PK);
            }
            else
            {
                query.SetPrimaryKey = (pk, q) => orderItem.PK = pk;
            }
            return query;
        }

        private NonQueryWithParameters GetOrderQuery(Order order)
        {
            NonQueryWithParameters query = (order.PK > 0) ?
                new NonQueryWithParameters(SQLUpdateOrder) :
                new NonQueryWithParameters(SQLInsertOrder);
            query.Parameters.Add("Datetime", order.OrderDatetime);
            query.Parameters.Add("CustomerPK", order.Customer.PK);
            query.Parameters.Add("Status", order.Status);
            if (order.PK > 0)
            {
                query.Parameters.Add("PK", order.PK);
            }
            else
            {
                query.SetPrimaryKey = (pk, q) => order.PK = pk;
            }
            return query;
        }
        private Order CreateOrderFromDataReader(DbDataReader reader)
        {
            var order = new Order();
            order.PK = (int)reader["PK"];
            order.OrderDatetime = (DateTime)reader["OrderDatetime"];
            order.Customer = null;
            order.Status = (OrderStatus)reader["Status"];
            return order;
        }
        private OrderItem CreateOrderItemFromDataReader(DbDataReader reader)
        {
            var orderItem = new OrderItem();
            orderItem.PK = (int)reader["PK"];
            orderItem.Order = null;
            orderItem.Quantity = (int)reader["Quantity"];
            orderItem.InventoryID = (int)reader["InventoryID"];
            orderItem.Price = (long)reader["Price"];
            return orderItem;
        }
        #endregion

        #region Customers...
        public Customer GetCustomerNoOrders(int customerId)
        {
            Customer customer = null;
            var processRow = new Func<DbDataReader, bool>(dr =>
                {
                    customer = CreateCustomerFromDataReader(dr);
                    return true;
                });
            var query = new SQLQueryWithParameters(SQLSelectSingleCustomer, processRow);
            query.Parameters.Add("CustomerId", customerId);
            queryHelper.ReadDataFromDB(query);
            return customer;
        }
        public Customer UpdateCustomer(Customer customer)
        {
            var query = GetCustomerQuery(customer);
            queryHelper.NonQueryToDB(query);
            return customer;
        }
        public Customer CreateCustomer(Customer customer)
        {
            var query = GetCustomerQuery(customer);
            queryHelper.NonQueryToDB(query);
            return customer;
        }
        private NonQueryWithParameters GetCustomerQuery(Customer customer)
        {
            var query = (customer.PK > 0) ?
                new NonQueryWithParameters(SQLUpdateSingleCustomer) :
                new NonQueryWithParameters(SQLCreateCustomer);
            query.Parameters.Add("Name", customer.Name);
            query.Parameters.Add("EmailAddress", customer.EmailAddress);
            query.Parameters.Add("Password", customer.Password);
            if (customer.PK > 0)
            {
                query.Parameters.Add("CustomerId", customer.PK);
            }
            else
            {
                query.SetPrimaryKey = (pk, q) => customer.PK = pk;
            }
            return query;
        }
        private Customer CreateCustomerFromDataReader(DbDataReader reader)
        {
            var customer = new Customer();
            customer.PK = (int)reader["PK"];
            customer.Name = (string)reader["Name"];
            customer.EmailAddress = (string)reader["EmailAddress"];
            customer.Password = (string)reader["Password"];
            return customer;
        }
        #endregion
    }
}
