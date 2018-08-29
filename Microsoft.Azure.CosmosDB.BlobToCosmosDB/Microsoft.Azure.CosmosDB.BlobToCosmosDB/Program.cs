namespace Microsoft.Azure.Documents.Tools.POC.BlobToCosmosDB
{
    using System;

    class Program
    {
        static void Main(string[] args)
        {
            DataLoader.WriteToCosmosDB();
            
            Console.WriteLine("Completed data load!");
            Console.ReadLine();
        }
    }
}
