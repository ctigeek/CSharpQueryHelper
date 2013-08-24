using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpQueryHelper.Example
{
    public enum OrderStatus {
        Ordered,
        BackOrdered,
        Shipped,
        Returned,
        Lost,
        Forgotten,
        Destroyed,
        Stolen
    }

    public class Order
    {
        public Order()
        {
            OrderItems = new List<OrderItem>();
        }
        public int PK { get; set; }
        public DateTime OrderDatetime { get; set; }
        public Customer Customer { get; set; }
        public OrderStatus Status { get; set; }

        public List<OrderItem> OrderItems { get; private set; }
    }
}
