using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace productsdailydownloadwebjob
{
    class Program
    {
        static void Main(string[] args)
        {
            DailyDownload process = new DailyDownload();
            process.Run();

        }
    }
}
