
namespace sasAzureblob

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
