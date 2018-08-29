using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Auth;

namespace Microsoft.Azure.Documents.Tools.POC.BlobToCosmosDB
{
    internal sealed class BlobRetriever
    {
        /// <summary>
        /// Local folder into which CSV files will be download from Blob Storage
        /// </summary>
        private static string CSVFolder = Path.Combine(Directory.GetCurrentDirectory(), "DownloadFromBlob");

        /// <summary>
        /// CloudBlobContainer object to access individual containers within the Blob Storage account
        /// </summary>
        private CloudBlobContainer BlobContainer;

        /// <summary>
        /// Connection string for the Blob Storage account
        /// </summary>
        private string StorageAccountConnectionString;

        /// <summary>
        /// Initializes a new BlobRetriever instance
        /// </summary>
        /// <param name="StorageAccountConnectionString"> Connection string to access the CSV files from Blob Storage </param>
        public BlobRetriever(string StorageAccountConnectionString)
        {
            string BlobStorageContainerName = ConfigurationManager.AppSettings["StorageAccountContainerName"];
            this.StorageAccountConnectionString = StorageAccountConnectionString;

            CloudStorageAccount StorageAccount =
                CloudStorageAccount.Parse(this.StorageAccountConnectionString);

            CloudBlobClient BlobClient = StorageAccount.CreateCloudBlobClient();
            BlobContainer = BlobClient.GetContainerReference(BlobStorageContainerName);
        }

        /// <summary>
        /// Downloads the specified file name (blob name) from the Azure Storage Account into a local folder
        /// </summary>
        /// 
        /// <param name="fileName"> List of file names to download from Cosmos DB </param>
        public void DownloadCsvFromBlobStorage(string FileName)
        {
            if (!bool.Parse(ConfigurationManager.AppSettings["FilesAreGZipped"]))
            {
                FileName = FileName.Replace(".gzip", ".csv");
            }

            Console.WriteLine("fileName = {0}", FileName);

            CloudBlockBlob BlockBlob = this.BlobContainer.GetBlockBlobReference(FileName);

            BlockBlob.DownloadToFile(Path.GetFullPath(Path.Combine(CSVFolder, FileName)), FileMode.OpenOrCreate);

            if (bool.Parse(ConfigurationManager.AppSettings["FilesAreGZipped"]))
            {
                FileName = Path.GetFullPath(Path.Combine(CSVFolder, FileName));
                string newFilePath = FileName.Replace(".gzip", ".csv");

                using (FileStream decompressedFileStream = File.Create(newFilePath))
                {
                    using (GZipStream decompressionStream = new GZipStream(File.OpenRead(FileName), CompressionMode.Decompress))
                    {
                        decompressionStream.CopyTo(decompressedFileStream);
                    }
                }
            }
        }

        /// <summary>
        /// For a list of file names (blobs), this method iterates through all corresponding Blobs within the container 
        /// and downloads them to a local folder.
        /// 
        /// </summary>
        /// <param name="FileNames"> List of file names in Blob Storage to download and ingest into Cosmos DB </param>
        public void DownloadAllCsvFilesFromBlobStorage(List<string> FileNames)
        {
            string BlobStorageContainerName = ConfigurationManager.AppSettings["StorageAccountContainerName"];

            if(FileNames.Count == 0)
            {
                foreach (CloudBlockBlob EachBlob in this.BlobContainer.ListBlobs())
                {
                    DownloadCsvFromBlobStorage(EachBlob.Name);
                    Console.WriteLine("Successfully downloaded Source file : {0}", EachBlob.Name);
                }
            }
            else
            {
                foreach (string FileName in FileNames)
                {
                    DownloadCsvFromBlobStorage(FileName);
                    Console.WriteLine("Successfully downloaded Source file : {0}", FileName);
                }
            }
        }
    }
}
