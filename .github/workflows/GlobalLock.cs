using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Medallion.Threading.Azure;
using System;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

public class GlobalLock
{
    private readonly BlobClient _blobClient;
    private readonly TimeSpan _leaseDuration = TimeSpan.FromSeconds(25);
    private readonly BlobContainerClient _blobContainer;
    public GlobalLock(string connectionString, string containerName, string blobName)
    {
        _blobClient = new BlobClient(connectionString, containerName, blobName);
        _blobContainer = new BlobContainerClient(connectionString, "spenda-integration");
        //_blobClient = new BlobServiceClient(connectionString);
    }

    //public async Task<BlobLeaseClient> AcquireLockAsync()
    
    private void Sleep () => Thread.Sleep(1000);
    public async Task<BlobLeaseClient> GetLeaseClient()
    {
        if (!await _blobClient.ExistsAsync())
        {
            // Create the blob with default content if it doesn't exist
            await _blobContainer.CreateIfNotExistsAsync();
            Console.WriteLine($"Blob created with default content.");
        }
        var blobleaseClient = _blobClient.GetBlobLeaseClient();
        //Response<BlobLease> response = await leaseClient.AcquireAsync(duration: TimeSpan.FromSeconds(10));
        return blobleaseClient;
    }

    public async Task ReleaseBlobLeaseAsync(BlobLeaseClient blobLeaseClient)
    {
        await blobLeaseClient.ReleaseAsync();
    }
    public async Task<AzureBlobLeaseDistributedLock?> AcquireLockAsync()
    {
        //var client = await _blobClient.CreateBlobContainerAsync("lock-lease");
        try
        {
            var completeBlobName = _blobClient.AccountName + " - " + _blobClient.Name;
            //var exists = _blobClient.Exists();
            if (!_blobContainer.Exists()) await _blobContainer.CreateAsync();

            
            AzureBlobLeaseDistributedLock lockBlob;
            lockBlob = new AzureBlobLeaseDistributedLock(_blobContainer, "SampleLease.txt") ;

            return lockBlob;
            //Console.WriteLine("lockblob returned {0}", lockBlob.Name);

        }

        catch (Exception ex)
        {
            return null;
        }
        /*
        try
        {
            await leaseClient.AcquireAsync(_leaseDuration);
            return leaseClient;
        }
        catch (Azure.RequestFailedException)
        {
            // Lease couldn't be acquired
            return null;
        }
        */
    }

    public async Task ReleaseLockAsync(BlobLeaseClient leaseClient)
    {
        await leaseClient.ReleaseAsync();
    }
}