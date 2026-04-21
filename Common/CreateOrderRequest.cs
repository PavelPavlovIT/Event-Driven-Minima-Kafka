using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public class CreateOrderRequest
    {
        public string CustomerName { get; set; }
        public decimal Amount { get; set; }
    }
}
