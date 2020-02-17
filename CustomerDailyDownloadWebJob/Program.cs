using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomerDailyDownloadWebJob
{
    class Program
    {
        static void Main(string[] args)
        {
            dailydownload process = new dailydownload();
            process.Run();

        }
    }
}
