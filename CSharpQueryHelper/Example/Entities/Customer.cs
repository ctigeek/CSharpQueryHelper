using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpQueryHelper.Example
{
    public class Customer
    {
        public Customer()
        {
            Orders = new List<Order>();
        }
        public int PK { get; set; }
        public string Name { get; set; }
        public string EmailAddress { get; set; }
        public string Password { get; set; }

        public List<Order> Orders { get; private set; }
    }
}
