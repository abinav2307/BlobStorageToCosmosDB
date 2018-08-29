using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Configuration;
using System.Globalization;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;

namespace Microsoft.Azure.Documents.Tools.POC.BlobToCosmosDB
{
    internal sealed class DataLoader
    {
        /// <summary>
        /// Local folder into which the CSV files from Blob Storage are downloaded
        /// </summary>
        private static string CsvFolder = Path.Combine(Directory.GetCurrentDirectory(), "DownloadFromBlob");

        /// <summary>
        /// Blob Storage Container name (which contains the CSV files to ingest into Cosmos DB)
        /// </summary>
        private static string blobStorageContainerName = ConfigurationManager.AppSettings["StorageAccountContainerName"];

        /// <summary>
        /// This method converts the already downloaded CSV files into Cosmos DB
        /// </summary>
        /// <param name="DownloadDestinationPath"> Local folder containing the downloaded CSV files from Blob Storage</param>
        /// <param name="StringFields"> List of fields to be treated as strings</param>
        /// <param name="PKFields"> List of fields to be used when creating/simulating a composite partition key</param>
        /// <returns></returns>
        private static List<string> CreateListOfJsonsFromCsv(
            string downloadDestinationPath,
            Dictionary<string, string> stringFields,
            List<string> pKFields)
        {
            downloadDestinationPath = downloadDestinationPath.Replace(".gzip", ".csv");
            string eachLineInCsv;

            StreamReader fileReader = new StreamReader(downloadDestinationPath);

            int delimiter = 44;
            int lineNumber = 0;

            char delimitingChar = Convert.ToChar(delimiter);
            string[] commaSeparatedFieldValues;
            string[] fieldNames = null;

            Console.WriteLine("Beginning massaging of file: {0} at: {1}", downloadDestinationPath, DateTime.Now.ToString());

            List<string> documentsToWrite = new List<string>();
            while ((eachLineInCsv = fileReader.ReadLine()) != null)
            {
                eachLineInCsv = eachLineInCsv.Replace("\"", "");

                if (lineNumber == 0)
                {
                    if (!bool.Parse(ConfigurationManager.AppSettings["FirstRowContainsColumnHeaders"]))
                    {
                        fieldNames = ConfigurationReader.GetListOfFieldNames();
                    }
                    else
                    {
                        fieldNames = eachLineInCsv.Split(delimitingChar);
                    }

                    lineNumber++;
                    continue;
                }

                commaSeparatedFieldValues = eachLineInCsv.Split(delimitingChar);

                if (fieldNames.Length != commaSeparatedFieldValues.Length)
                {
                    continue;
                }

                try
                {
                    string JsonText = CosmosDBCsvStringToJsonConverter.JsonTextFromCsv(eachLineInCsv, fieldNames, commaSeparatedFieldValues, stringFields, pKFields);
                    documentsToWrite.Add(JsonText);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception encountered while processing CSV line: {0}. Original exception message was: {1}", eachLineInCsv, ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }
            }

            fileReader.Close();

            return documentsToWrite;
        }

        private static void CreateDirectoryIfNotExists(string directoryName)
        {
            if (!Directory.Exists(directoryName))
            {
                Directory.CreateDirectory(directoryName);
            }
        }

        /// <summary>
        /// This method performs the following operations:
        /// <list type="bullet"
        ///     <item>
        ///         <description> 1. Downloads CSV files from the Blob Storage account into a local folder
        ///         </description>
        ///     </item>
        ///     <item>
        ///         <description> 2. Parse each row/line in the CSV file into JSON documents to be written into Cosmos DB
        ///         </description>
        ///     </item>
        ///     <item>
        ///         <description> 3. Use the BulkExecutor library to ingest the JSON documents from the CSV file into Cosmos DB
        ///         </description>
        ///     </item>
        ///     <item>
        ///         <description> 4. Delete the CSV file downloaded into the local folder
        ///         </description>
        ///     </item>
        /// </list>
        /// </summary>
        public static void WriteToCosmosDB()
        {
            // 1. Create the folder within which the CSV (zipped or unzipped) file(s) should be downloaded, if it does not already exist
            CreateDirectoryIfNotExists(CsvFolder);

            // 2. Get the connection string for the storage account from which to retrieve the input data
            string StorageAccountConnectionString = ConfigurationManager.AppSettings["StorageAccountConnectionString"];
            
            List<string> inputFiles = ConfigurationReader.GetListOfInputFiles();
            List<string> pKFields = ConfigurationReader.GetListOfPKFields();
            Dictionary<string, string> stringCharacters = ConfigurationReader.GetMapOfStringFieldsInDataset();
            
            BlobRetriever blobRetriever = new BlobRetriever(StorageAccountConnectionString);
            BulkImportDataLoad bulkImporter = new BulkImportDataLoad();

            foreach (string inputFile in inputFiles)
            {
                string localOutputFileName = inputFile;
                Console.WriteLine("Data Source file is {0}", localOutputFileName);

                if (!File.Exists(Path.Combine(CsvFolder, localOutputFileName)))
                {
                    blobRetriever.DownloadCsvFromBlobStorage(localOutputFileName);
                }
                
                // Parse the downloaded CSV and write the output to a single file with all valid JSONs to insert into Cosmos DB
                string downloadDestinationPath = Path.Combine(CsvFolder, localOutputFileName);

                List<string> documentsToWrite = CreateListOfJsonsFromCsv(downloadDestinationPath, stringCharacters, pKFields);

                Console.WriteLine("About to send the list of documents to write to BulkImportTool");
                bulkImporter.DataImportForMultipleTemplates(documentsToWrite);

                localOutputFileName = localOutputFileName.Replace(".gzip", ".csv");
                File.Delete(Path.Combine(CsvFolder, localOutputFileName));
            }
        }
    }
}
