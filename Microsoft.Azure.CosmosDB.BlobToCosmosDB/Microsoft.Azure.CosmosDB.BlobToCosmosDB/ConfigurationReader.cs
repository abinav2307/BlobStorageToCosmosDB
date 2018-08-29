using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.Tools.POC.BlobToCosmosDB
{
    internal sealed class ConfigurationReader
    {
        /// <summary>
        /// Config specifying the number fields in the CSV file 
        /// This config is used when column headers are not present in the CSV file
        /// </summary>
        private static readonly string NumFieldsInCsvConfigString = "NumFieldsInCSV";

        /// <summary>
        /// Config specifying the number of fields to concatenate into the composite key
        /// </summary>
        private static readonly string NumFieldsInPKConfigString = "NumberOfFieldsInPK";

        /// <summary>
        /// Config specifying the name of the partition key field, for the collection into which data is being ingested
        /// </summary>
        private static readonly string PKFieldNamePrefixConfigString = "PKField";

        /// <summary>
        /// Config specifying the number of fields in the CSV file, to be treated as a string when converting to JSON
        /// </summary>
        private static readonly string NumberOfStringFieldsInDatasetConfigString = "NumberOfStringFieldsInDataset";

        /// <summary>
        /// Field name prefix (in App.config) required to specify each field that should be treated as a string
        /// </summary>
        private static readonly string StringFieldNamePrefixConfigString = "StringField";

        /// <summary>
        /// Config specifying the number of files to read from the container in the Storage account
        /// </summary>
        private static readonly string NumCsvFilesToReadConfigString = "NumberOfFilesToRead";

        /// <summary>
        /// File name prefix (in App.config) required to specify each file name to be fetched from the storage account.
        /// </summary>
        private static readonly string FileNamePrefix = "FileName";

        /// <summary>
        /// Field name prefix (in App.config) required to specifc the name of each field, when the CSV file does not contain column headers
        /// </summary>
        private static readonly string FieldNamePrefix = "Field";

        /// <summary>
        /// When there are several CSV files to import, the user has the option to specify
        /// a subset of files to ingest into Cosmos DB.
        /// 
        /// The format is as below:
        /// <add key="NumberOfFilesToRead" value="2" />
        /// <add key = "FileName1" value="FileName1.csv"/>
        /// <add key = "FileName2" value="FileName2.csv"/>
        /// 
        /// The first config indicates the number of file names to read, followed
        /// by the names of each of the files to parse and ingest into Cosmos DB.
        /// 
        /// </summary>
        /// <returns></returns>
        public static List<string> GetListOfInputFiles()
        {
            int NumFilesToRead = int.Parse(ConfigurationManager.AppSettings[NumCsvFilesToReadConfigString]);
            List<string> InputFiles = new List<string>();

            for (int EachFileToRead = 1; EachFileToRead <= NumFilesToRead; EachFileToRead++)
            {
                string KeyNameInConfig = string.Concat(FileNamePrefix, EachFileToRead);
                InputFiles.Add(ConfigurationManager.AppSettings[KeyNameInConfig]);
            }

            return InputFiles;
        }

        /// <summary>
        /// When migrating data from CSV files into Cosmos DB, if the first row of the CSV
        /// does not contain the column/field names, the user has the option to specify the field
        /// names in order, in the App.config file.
        /// 
        /// The format is as below:
        /// <add key = "NumFieldsInCSV" value="4" />
        /// <add key = "Field1" value="src_sys_id"/>
        /// <add key = "Field2" value="src_trx_dt"/>
        /// <add key = "Field3" value="src_ship_from_loc_cd"/>
        /// <add key = "Field4" value="src_ship_to_loc_cd"/>
        /// 
        /// The first config indicates the number of fields in the CSV files, followed
        /// by the names of each of the column/fields.
        /// 
        /// </summary>
        /// <returns></returns>
        public static string[] GetListOfFieldNames()
        {
            int NumFieldsInCSV = int.Parse(ConfigurationManager.AppSettings[NumFieldsInCsvConfigString]);
            string[] fieldNames = new string[NumFieldsInCSV];

            for (int EachFieldToUseInPK = 0; EachFieldToUseInPK < NumFieldsInCSV; EachFieldToUseInPK++)
            {
                string KeyNameInConfig = string.Concat(FieldNamePrefix, (EachFieldToUseInPK + 1));
                fieldNames[EachFieldToUseInPK] = ConfigurationManager.AppSettings[KeyNameInConfig];
            }

            return fieldNames;
        }

        /// <summary>
        /// Once the field names have been determined, the next step is for the tool to be aware of
        /// the fields that need to be treated as strings, so as to enclose the values
        /// in \". 
        /// 
        /// The format is as below:
        /// <add key= "NumberOfStringFieldsInDataset" value="2" />
        /// <add key = "StringField1" value="stringField1"/>
        /// <add key = "StringField2" value="stringField2"/>
        /// 
        /// The first config indicates the number of fields to be treated as strings, followed
        /// by the names of each of the fields to be treated as strings. Any field name
        /// not included in this list will be treated as a number.
        /// 
        /// </summary>
        /// <returns></returns>
        public static Dictionary<string, string> GetMapOfStringFieldsInDataset()
        {
            Dictionary<string, string> stringCharacters = new Dictionary<string, string>();

            int NumberOfStringsFieldsInDataset = int.Parse(ConfigurationManager.AppSettings[NumberOfStringFieldsInDatasetConfigString]);

            for (int EachStringField = 1; EachStringField <= NumberOfStringsFieldsInDataset; EachStringField++)
            {
                string KeyNameInConfig = string.Concat(StringFieldNamePrefixConfigString, EachStringField);
                stringCharacters.Add(ConfigurationManager.AppSettings[KeyNameInConfig], "string");
            }

            return stringCharacters;
        }

        /// <summary>
        /// One of the columns in the CSV file must be chosen as the partition key for the collection
        /// into which the data is being ingested. If multiple columns exist (composite key), 
        /// the partition key field will be constructed by concatenating the fields specified
        /// by an underscore '_'
        /// 
        /// The format is as below:
        /// <add key="NumberOfFieldsInPK" value="2" />
        /// <add key = "PKField1" value="field1"/>
        /// <add key = "PKField2" value="field2"/>
        /// 
        /// The first config indcates the number of fields to concatenate into the partition key, followed
        /// by the names of each of the fields to use as part of the partition key.
        /// 
        /// </summary>
        /// <returns></returns>
        public static List<string> GetListOfPKFields()
        {
            int NumberOfFieldsToUseInPK = int.Parse(ConfigurationManager.AppSettings[NumFieldsInPKConfigString]);
            List<string> PKFieldNames = new List<string>();

            for (int EachFieldToUseInPK = 1; EachFieldToUseInPK <= NumberOfFieldsToUseInPK; EachFieldToUseInPK++)
            {
                string KeyNameInConfig = string.Concat(PKFieldNamePrefixConfigString, EachFieldToUseInPK);
                PKFieldNames.Add(ConfigurationManager.AppSettings[KeyNameInConfig]);
            }

            return PKFieldNames;
        }
    }
}
