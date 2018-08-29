using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.Documents.Tools.POC.BlobToCosmosDB
{
    internal sealed class CosmosDBCsvStringToJsonConverter
    {
        public static string JsonTextFromCsv(string Csv, string[] fieldNames, string[] commaSeparatedFieldValues, Dictionary<string, string> stringFields, List<string> pKFields)
        {
            // Extract the name of the partition key field
            string PKFieldName = ConfigurationManager.AppSettings["PartitionKeyFieldName"];

            // Maintain the values of the fields that need to be used to create a composite key
            Dictionary<string, string> PKFieldMapping = new Dictionary<string, string>();
            PKFieldMapping.Add(PKFieldName, "");

            // Maintain a map of field, value pairs for each line in the CSV file to ingest into Cosmos DB
            Dictionary<string, object> JObjectMapping = new Dictionary<string, object>();

            for (int EachFieldIndex = 0; EachFieldIndex < fieldNames.Length; EachFieldIndex++)
            {
                if (pKFields.Contains(fieldNames[EachFieldIndex]))
                {
                    if (!string.IsNullOrEmpty(commaSeparatedFieldValues[EachFieldIndex]) && !commaSeparatedFieldValues[EachFieldIndex].Equals("\"\""))
                    {
                        PKFieldMapping[PKFieldName] = string.Concat(PKFieldMapping[PKFieldName], "_", commaSeparatedFieldValues[EachFieldIndex]);
                    }
                }
                else
                {
                    // Treat string fields
                    if (stringFields.ContainsKey(fieldNames[EachFieldIndex]))
                    {
                        if (commaSeparatedFieldValues[EachFieldIndex].Equals("\"\""))
                        {
                            JObjectMapping.Add(fieldNames[EachFieldIndex], "\"\"");
                        }
                        else
                        {
                            JObjectMapping.Add(fieldNames[EachFieldIndex], commaSeparatedFieldValues[EachFieldIndex]);
                        }
                    }
                    else
                    {
                        // Treat numeric fields
                        if (commaSeparatedFieldValues[EachFieldIndex].Length == 0)
                        {
                            JObjectMapping.Add(fieldNames[EachFieldIndex], 0.0);
                        }
                        else
                        {
                            double valueToWrite = int.Parse(commaSeparatedFieldValues[EachFieldIndex]);
                            JObjectMapping.Add(fieldNames[EachFieldIndex], valueToWrite);
                        }
                    }
                }
            }

            if (pKFields.Count > 0)
            {
                JObjectMapping.Add(PKFieldName, PKFieldMapping[PKFieldName]);
            }

            return JsonConvert.SerializeObject(JObjectMapping);
        }
    }
}
