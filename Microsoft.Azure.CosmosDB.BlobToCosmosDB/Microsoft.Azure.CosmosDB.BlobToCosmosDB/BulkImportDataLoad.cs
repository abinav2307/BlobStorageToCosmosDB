using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;

using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;

using Newtonsoft.Json;

namespace Microsoft.Azure.Documents.Tools.POC.BlobToCosmosDB
{
    internal sealed class BulkImportDataLoad
    {
        /// <summary>
        /// DocumentClient instance to be used for the ingestion
        /// </summary>
        private DocumentClient Client;

        /// <summary>
        /// Name of the Cosmos DB collection into which to ingest the data imported from CSV files
        /// </summary>
        private string CollectionName;

        /// <summary>
        /// Name of the Cosmos DB database into which to ingest the data imported from CSV files
        /// </summary>
        private string DatabaseName;

        /// <summary>
        /// The Cosmos DB endpoint write to
        /// </summary>
        private string Endpoint;

        /// <summary>
        /// The Primary Key for the Cosmos DB endpoint to write to
        /// </summary>
        private string AuthKey;

        /// <summary>
        /// ConnectionPolicy associated with the DocumentClient
        /// </summary>
        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        /// <summary>
        /// Initializes a new Instance of the BulkImportDataLoad class, which bulk ingests the data download from Blob Storage into Cosmos DB
        /// </summary>
        public BulkImportDataLoad()
        {
            this.Endpoint = ConfigurationManager.AppSettings["EndPointUrl"];
            this.AuthKey = ConfigurationManager.AppSettings["AuthorizationKey"];
            this.DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
            this.CollectionName = ConfigurationManager.AppSettings["CollectionName"];

            try
            {
                Client = new DocumentClient(new Uri(Endpoint), AuthKey, ConnectionPolicy);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception while creating DocumentClient. Original  exception message was: {0}", ex.Message);
            }
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested database.</returns>
        private Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested collection.</returns>
        private DocumentCollection GetCollectionIfExists(string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(this.Client, databaseName) == null)
            {
                return null;
            }

            return this.Client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// This method uses the Cosmos DB BulkExecutor library to bulk ingest the input list of JSON documents
        /// </summary>
        /// <param name="documentsToImport"> List of documents to bulk ingest into Cosmos DB </param>
        public async void DataImportForMultipleTemplates(List<string> documentsToImport)
        {
            DocumentCollection collection = GetCollectionIfExists(this.DatabaseName, this.CollectionName);
            if (collection == null)
            {
                throw new Exception("The collection does not exist");
            }

            BulkExecutor bulkExecutor = new BulkExecutor(this.Client, collection);
            await bulkExecutor.InitializeAsync();

            BulkImportResponse bulkImportResponse = null;
            long totalNumberOfDocumentsInserted = 0;
            double totalRequestUnitsConsumed = 0;
            double totalTimeTakenSec = 0;

            try
            {
                bulkImportResponse = await bulkExecutor.BulkImportAsync(documentsToImport, false, false);
            }
            catch (DocumentClientException de)
            {
                Console.WriteLine("Document client exception while execting bulk insert. Stack trace: \n {0}", de.StackTrace);
                Console.ReadLine();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception thrown while executing bulk insert. Stack trace:\n {0}", e.StackTrace);
                Console.ReadLine();
            }

            Console.WriteLine(String.Format("\nSummary for write."));
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec)",
                bulkImportResponse.NumberOfDocumentsImported,
                Math.Round(bulkImportResponse.NumberOfDocumentsImported / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                Math.Round(bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                bulkImportResponse.TotalTimeTaken.TotalSeconds));
            Console.WriteLine(String.Format("Average RU consumption per document: {0}",
                (bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.NumberOfDocumentsImported)));
            Console.WriteLine("---------------------------------------------------------------------\n ");

            totalNumberOfDocumentsInserted += bulkImportResponse.NumberOfDocumentsImported;
            totalRequestUnitsConsumed += bulkImportResponse.TotalRequestUnitsConsumed;
            totalTimeTakenSec += bulkImportResponse.TotalTimeTaken.TotalSeconds;            
        }
    }
}
