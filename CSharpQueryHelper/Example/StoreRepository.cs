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

        private Customer CreateCustomerFromDataReader(DbDataReader reader)
        {
            var customer = new Customer();
            customer.PK = (int)reader["PK"];
            customer.Name = (string)reader["Name"];
            customer.EmailAddress = (string)reader["EmailAddress"];
            customer.Password = (string)reader["Password"];
            return customer;
        }

        private Order CreateOrderFromDataReader(DbDataReader reader)
        {
            var order = new Order();
            order.PK = (int) reader["PK"];
            order.OrderDatetime = (DateTime) reader["OrderDatetime"];
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
    }
}
