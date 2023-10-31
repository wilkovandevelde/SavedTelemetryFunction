using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SavedTelemetryFunction
{
    public class SavedTelemetry
    {

        public string deviceId { get; set; }
        public DateTime timeStamp { get; set; }
        public Int64? lokatie { get; set; }
        public string model { get; set; }
        public string status { get; set; }
        public string gps { get; set; }
    }
}
