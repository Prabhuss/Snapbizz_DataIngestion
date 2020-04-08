using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net.Mail;


namespace productsdailydownloadwebjob
{
    class DailyDownload
    {

        public void Run()
        {
            doJob();
        }


        static void doJob()
        {



            string downlloadDirPath = ConfigurationManager.AppSettings["downlloadDirPath"];
            string blobDirPath = downlloadDirPath;



            /* for daily update of record please make  startDateFromToday = 841 and  endDateFromToday = 1 */

            string startDateFromToday = ConfigurationManager.AppSettings["startDateFromToday"];
            int startNoOfDayFromToday = Int32.Parse(startDateFromToday);

            string endDateFromToday = ConfigurationManager.AppSettings["endDateFromToday"];
            int endNoOfDayFromToday = Int32.Parse(endDateFromToday);

            string[] invoiceList = new string[] { "products" };
            Stopwatch sw = new Stopwatch();
            foreach (string invoicePath in invoiceList)
            {

                DateTime begindate = DateTime.Now;
                DateTime endDate = DateTime.Now;

                List<DateTime> dateList = new List<DateTime>();

                begindate = begindate.AddDays(startNoOfDayFromToday - 1);
                endDate = endDate.AddDays(endNoOfDayFromToday);
                //  Console.WriteLine("Begin date :"+ begindate);
                //  Console.WriteLine("End date :"+ endDate);

                if (begindate > endDate)
                {
                    Console.WriteLine(" Begin Date is larger than End Date");
                }

                else
                {
                    Console.WriteLine(" Going to download file from : " + begindate.AddDays(1) + " till:   " + endDate);
                }



                while (begindate <= endDate)
                {
                    begindate = begindate.AddDays(1);
                    dateList.Add(begindate);

                }

                foreach (DateTime newDate in dateList)
                {
                    string Date = newDate.ToString("dd-MMM-yyyy");
                    string exe = ".csv";
                    string fileName = String.Concat(Date, exe);
                    string blobPath = fileName;
                    Console.WriteLine("BlobPath is: " + blobPath + " And Dir is :  " + invoicePath);


                    string sasUri = "https://snapbizzanalyitcsdata.blob.core.windows.net/docdbdumo/" +
                   invoicePath + "/" + blobPath +
              "?sv = 2019 - 02 - 02 & ss = b & srt = sco & sp = rl & se = 2020 - 06 - 28T18: 29:57Z &" +
              "st = 2020 - 03 - 28T12: 29:57Z & spr = https & sig = VksDOTUJhLKZgX3fTD2kpFnjm % 2BBQeuErtTSuhkbA % 2Bl8 % 3D";




 


                    CloudBlockBlob blob = new CloudBlockBlob(new Uri(sasUri));
                    try
                    {
                        if (blob.Exists())
                        {
                            Console.WriteLine(" great blob exists");
                            sw.Start();
                            var ms = new MemoryStream();
                            blob.DownloadToStream(ms);
                            sw.Stop();

                            Console.WriteLine("Downladed the Blob and it took: " + sw.Elapsed + "  (hh:mm:ss.sssssss)");

                            // Now Data has been download in memory, Let us build Data Table. 
                            DataTable csvData = new DataTable();
                            csvData = GetDataTabletFromCSVFile(ms, blobPath);
                            // Now Ingest it to SQl Table
                            if (csvData != null)
                            {
                                string result = InsertDataIntoSQLServerUsingSQLBulkCopy(csvData);
                                RumEmailer(result);
                            }
                        }
                        else
                        {
                            Console.WriteLine("Blob " + blobPath + " does not exist for: " + invoicePath);
                        }
                        Console.WriteLine();
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode == 403)
                        {
                            Console.WriteLine("Create operation failed for SAS {0}", sasUri);
                            Console.WriteLine("Additional error information: " + e.Message);
                            Console.WriteLine();
                        }
                        else
                        {
                            Console.WriteLine(e.Message);
                            // Console.ReadLine();
                            throw;
                        }
                    }
                }
                //Console.ReadLine();


            }
        }



        public static DataTable GetDataTabletFromCSVFile(MemoryStream msCsv, string blobName)
        {
            DataTable csvData = new DataTable();
            msCsv.Seek(0, SeekOrigin.Begin);
            try
            {
                using (StreamReader csvReader = new StreamReader(msCsv,
                                           System.Text.Encoding.UTF8,
                                           true))
                {
                    string currentLine;
                    int rowNum = 0;
                    while ((currentLine = csvReader.ReadLine()) != null)
                    {

                        //Console.WriteLine(" Read Line is " + currentLine);
                        string[] colFields = currentLine.Split(new char[] { ',' }, StringSplitOptions.None);
                        if (rowNum == 0)
                        {
                            foreach (string column in colFields)
                            {
                                DataColumn datacolumn = new DataColumn(column);
                                datacolumn.AllowDBNull = true;
                                csvData.Columns.Add(datacolumn);
                            }
                            // Add file name col

                            csvData.Columns.Add("csvFileName");
                            csvData.Columns.Add("csvRowNum");
                            rowNum++;
                            continue;
                        }

                        if (rowNum % 1000 == 0)
                        {
                            Console.WriteLine("Read " + rowNum + "  Lines ");
                        }

                        string[] fieldData = colFields;

                        string[] finalFieldData = new string[fieldData.Length + 2];
                        int j = 0;
                        for (int i = 0; i < fieldData.Length; ++i)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                            finalFieldData[i] = fieldData[i];
                            j = i;

                        }

                        /* insert file name */
                        finalFieldData[j + 1] = blobName;
                        /* insert row name */
                        finalFieldData[j + 2] = rowNum.ToString();
                        rowNum++;

                        // If number of field are not same as number of header col,
                        // don't add it to DataTable, just log it, we will handle this seprately 
                        if (finalFieldData.Length != csvData.Columns.Count)
                        {
                            Console.WriteLine(" Actual Row Data and  Col Count does not match, Please make Manual entry for Row number :" + rowNum);

                        }
                        else
                        {
                            csvData.Rows.Add(finalFieldData);
                            //Console.WriteLine("addd new row number : " + csvData.Rows.Count);
                        }


                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Something went wrong while making Data Table  at   " + csvData.Rows.Count + " Error is  " + ex.GetType().ToString());

                return null;

            }
            return csvData;
        }

        static void RumEmailer(string result)

        {

            string errorTex = "WebJob Unsucessful. Reason: " + result;
            MailMessage mail = new MailMessage();
            SmtpClient SmtpServer = new SmtpClient("smtp.gmail.com");
            mail.From = new MailAddress("pytechnologies.pvt.ltd@gmail.com");

            mail.To.Add("prabhuss@pytechno.com");
            mail.Bcc.Add("ifraanjum0309@gmail.com");


            mail.Subject = "product web job notification";
            if (result == "1")
                mail.Body = "<html>WebJob Successful</html>";

            else
                mail.Body = "<html>'" + errorTex + "' </html>";

            mail.IsBodyHtml = true;
            SmtpServer.Port = 587;



            SmtpServer.Credentials = new System.Net.NetworkCredential("pytechnologies.pvt.ltd@gmail.com", "pytech246");

            SmtpServer.EnableSsl = true;
            SmtpServer.Send(mail);



        }


        private static void OnSqlRowsCopied(
        object sender, SqlRowsCopiedEventArgs e)
        {
            Console.WriteLine("Copied {0} so far...", e.RowsCopied);
        }

        public static string InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData)
        {

            try
            {
                string connection1 = ConfigurationManager.ConnectionStrings["MyConnection"].ConnectionString;
                using (SqlConnection dbConnection = new SqlConnection(connection1))
                {
                    dbConnection.Open();
                    using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                    {
                        s.DestinationTableName = "[sb_products]";
                        s.BatchSize = 500;


                        s.ColumnMappings.Add("store_id", "store_id");
                        s.ColumnMappings.Add("created_at", "created_at");
                        s.ColumnMappings.Add("product_code", "product_code");
                       
                        s.ColumnMappings.Add("is_deleted", "is_deleted");
                        s.ColumnMappings.Add("updated_at", "updated_at");
                        s.ColumnMappings.Add("name", "name");
                        s.ColumnMappings.Add("server_last_modified", "server_last_modified");
                        s.ColumnMappings.Add("id", "id");
                        s.ColumnMappings.Add("is_gdb", "is_gdb");
                        s.ColumnMappings.Add("vat_rate", "vat_rate");
                       
                        if (csvFileData.Columns.Contains("uom"))
                        {
                            s.ColumnMappings.Add("uom", "uom");

                        }
                        if (csvFileData.Columns.Contains("barcode"))
                        {
                            s.ColumnMappings.Add("barcode", "barcode");

                        }



                        if (csvFileData.Columns.Contains("csvFileName"))
                        {
                            s.ColumnMappings.Add("csvFileName", "csvFileName");

                        }



                        if (csvFileData.Columns.Contains("csvRowNum"))
                        {
                            s.ColumnMappings.Add("csvRowNum", "csvRowNum");

                        }









                        s.SqlRowsCopied += new SqlRowsCopiedEventHandler(OnSqlRowsCopied);
                        s.NotifyAfter = 50000;

                        s.WriteToServer(csvFileData);
                    }
                }

                return 1.ToString();

            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine(ex.ToString());
                return ex.ToString();
            }

        }

    }
}
