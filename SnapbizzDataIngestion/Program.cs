using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;


namespace SnapbizzDataIngestion
{
    class Program
    {
        static void Main(string[] args)
        {

            try
            {
                string startDateFromToday = ConfigurationManager.AppSettings["startDateFromToday"];
                int startNoOfDayFromToday = Int32.Parse(startDateFromToday);

                string endDateFromToday = ConfigurationManager.AppSettings["endDateFromToday"];
                int endNoOfDayFromToday = Int32.Parse(endDateFromToday);

                DateTime begindate = DateTime.Now;
                DateTime endDate = DateTime.Now;

                List<DateTime> dateList = new List<DateTime>();

                begindate = begindate.AddDays(startNoOfDayFromToday - 1);
                endDate = endDate.AddDays(endNoOfDayFromToday);


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

                const string blobContainerName = "docdbdumo-v2";
                const string sasToken = "?sv=2018-03-28&ss=b&srt=sco&sp=rl&se=2020-03-29T16:18:36Z&";

                // Create a credential object from the SAS token then use this and the account name to create a cloud storage connection
                var accountSAS = new StorageCredentials(sasToken);
                var storageAccount = new CloudStorageAccount(accountSAS, "snapbizzanalyitcsdata", null, true);

                // Get the Blob storage container
                var blobClient = storageAccount.CreateCloudBlobClient();
                var container = blobClient.GetContainerReference(blobContainerName);

                // Fail if the container does not exist
                if (!container.Exists())
                {
                    Console.Error.WriteLine("Can't find container {0}", blobContainerName);
                    return;
                }
                else
                {
                    Console.WriteLine(" {0} exists ", blobContainerName);
                    foreach (DateTime newDate in dateList)
                    {
                        //formatting the date as the one in azure container
                        string formattedDate = newDate.ToString("dd-MMM-yyyy");
                        //gibing the directory reference of azure container 
                        var directory = container.GetDirectoryReference(@"invoices1/" + formattedDate);
                        //listing the folders
                        var folders = directory.ListBlobs().ToList();
                        if (folders.Count > 0)
                            Console.Write("great csv files exists for  ");

                        foreach (CloudBlockBlob blob in folders)
                        {
                            //downloading each blob to memory
                            Console.WriteLine(blob.Uri);
                            var ms = new MemoryStream();
                            blob.DownloadToStream(ms);
                            Console.WriteLine("Blob Downloaded!!!!......");

                        }
                    }


                }





            }


            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
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
                catch (System.Exception ex)
                {
                    Console.WriteLine(" Something went wrong while making Data Table  at   " + csvData.Rows.Count + " Error is  " + ex.GetType().ToString());

                    return null;

                }
                return csvData;
            }


    }
}



