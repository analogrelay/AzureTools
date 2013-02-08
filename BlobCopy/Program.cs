using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 4 || args.Length > 5)
            {
                Console.WriteLine("Usage: BlobCopy [source connection string] [source container] [destination connection string] [destination container]");
                Console.WriteLine("Add -! to the end to show what would be done without actually doing it");
                return;
            }

            // Collect Args 
            CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(args[0]);
            string sourceContainerName = args[1];
            CloudBlobClient sourceClient = sourceAccount.CreateCloudBlobClient();
            CloudBlobContainer sourceContainer = sourceClient.GetContainerReference(sourceContainerName);
            
            CloudStorageAccount destAccount = CloudStorageAccount.Parse(args[2]);
            string destContainerName = args[3];
            CloudBlobClient destClient = destAccount.CreateCloudBlobClient();
            CloudBlobContainer destContainer = destClient.GetContainerReference(destContainerName);
            destContainer.CreateIfNotExists();

            bool whatIf = args.Length == 5 && args[4] == "-!";
            
            
            // List blobs in the source account
            foreach(var sourceBlob in sourceContainer.ListBlobs(useFlatBlobListing: true).Cast<CloudBlockBlob>()) {
                if (!whatIf)
                {
                    // Get a reference to the destination blob
                    var destBlob = destContainer.GetBlockBlobReference(sourceBlob.Name);

                    // Get a temp file
                    var tempFileName = Path.GetTempFileName();
                    
                    // Download the source blob
                    using (var tempStream = File.OpenWrite(tempFileName))
                    {
                        sourceBlob.DownloadToStream(tempStream);
                    }

                    // Re-open the temp file and upload it to the dest blob
                    using (var tempStream = File.OpenRead(tempFileName))
                    {
                        destBlob.UploadFromStream(tempStream);
                    }

                    // Delete the temp file
                    File.Delete(tempFileName);
                }
                Console.WriteLine("Transferred " + sourceBlob.Name);
            }
        }
    }
}
