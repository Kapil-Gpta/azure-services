using Azure;
using Azure.Data.Tables;
using System.Diagnostics;

public class ConcurrentTableStorageManager
{
    private readonly TableClient _tableClient;
    private static object _locker = new object();
    //private string BlobConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
    private string BlobConnectionString=string.Empty;
    public ConcurrentTableStorageManager(string connectionString, string tableName)
    {
        BlobConnectionString = connectionString;
        _tableClient = new TableClient(connectionString, tableName);
    }

    public async Task<bool> UpdateEntityConcurrently<T>(string partitionKey, string rowKey, Func<T, T> updateFunction, int maxRetries = 5) where T : class, ITableEntity, new()
    {
        int attempts = 0;
        while (attempts < maxRetries)
        {
            var lockBlob = await new GlobalLock(BlobConnectionString, "spenda-integration", "SampleLease.txt").AcquireLockAsync();
            if (lockBlob == null) return false;
            try
            {
                using (var handle = lockBlob?.TryAcquire(timeout: TimeSpan.FromSeconds(12)))
                {
                    if (handle != null)
                    {
                        try
                        {
                            // Retrieve the current entity
                            //var response = _tableClient.GetEntity<T>(partitionKey, rowKey);
                            var response = await _tableClient.GetEntityAsync<T>(partitionKey, rowKey);
                            var entity = response.Value;

                            // Apply the update function
                            var updatedEntity = updateFunction(entity);

                            // Attempt to update the entity
                            //_tableClient.UpdateEntity(updatedEntity, entity.ETag, TableUpdateMode.Merge);
                            await _tableClient.UpdateEntityAsync(updatedEntity, entity.ETag, TableUpdateMode.Merge);
                            Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} successfully updated entity at: {DateTime.Now}.");
                            return true;
                        }
                        catch (RequestFailedException ex) when (ex.Status == 412) // Precondition Failed
                        {
                            Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} encountered a concurrency conflict. Retrying at..{DateTime.Now}");
                            attempts++;
                            await Task.Delay(TimeSpan.FromMilliseconds(100 * attempts)); // Exponential back-off
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} encountered an error: {ex.Message}");
                            return false;
                        }

                        finally
                        {
                            Console.WriteLine("finally called...{0}", Thread.CurrentThread.ManagedThreadId.ToString());
                            //await globalLock.ReleaseLockAsync(lease);
                        }
                    }
                }
            }

            catch (RequestFailedException ex) when (ex.Status == 412) // Precondition Failed
            {
                Console.WriteLine($"Retrying as : {Thread.CurrentThread.ManagedThreadId} encountered a concurrency conflict. Retrying at..{DateTime.Now}");
                attempts++;
                await Task.Delay(TimeSpan.FromMilliseconds(100 * attempts)); // Exponential back-off
            }
        }

        Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} failed to update after {maxRetries} attempts.");
        return false;
    }
    public async Task<bool> UpdateEntityConcurrentlyUsingLock<T>(string partitionKey, string rowKey, Func<T, T> updateFunction, int maxRetries = 5) where T : class, ITableEntity, new()
    {
        int attempts = 0;
        while (attempts < maxRetries)
        {
            try
            {
                lock (_locker)
                {
                    // Retrieve the current entity
                    var response = _tableClient.GetEntity<T>(partitionKey, rowKey);
                    var entity = response.Value;

                    // Apply the update function
                    var updatedEntity = updateFunction(entity);

                    // Attempt to update the entity
                    _tableClient.UpdateEntity(updatedEntity, entity.ETag, TableUpdateMode.Merge);
                    Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} successfully updated entity at: {DateTime.Now}.");
                    return true;
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 412) // Precondition Failed
            {
                Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} encountered a concurrency conflict. Retrying at..{DateTime.Now}");
                attempts++;
                await Task.Delay(TimeSpan.FromMilliseconds(100 * attempts)); // Exponential back-off
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} encountered an error: {ex.Message}");
                return false;
            }
        }

        Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} failed to update after {maxRetries} attempts.");
        return false;
    }

    public async Task<bool> UpdateEntityConcurrentlyUsingBlobLeaseClient<T>(string partitionKey, string rowKey, Func<T, T> updateFunction) where T : class, ITableEntity, new()
    {
        int attempts = 0;
        Console.WriteLine("In Blob lease client. Thread ID is => {0}",Thread.CurrentThread.ManagedThreadId);
        bool isCompleted = false; 
        while (!isCompleted)
        {
            try
            {
                var blobLeaseClient = await new GlobalLock(BlobConnectionString, "spenda-integration", "SampleLease.txt").GetLeaseClient();

                blobLeaseClient.Acquire(TimeSpan.FromSeconds(15));
                // Retrieve the current entity
                var response = _tableClient.GetEntity<T>(partitionKey, rowKey);
                var entity = response.Value;

                // Apply the update function
                var updatedEntity = updateFunction(entity);
                
                // Attempt to update the entity
                _tableClient.UpdateEntity(updatedEntity, entity.ETag, TableUpdateMode.Replace);
                Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} successfully updated entity using blob lease client at: {DateTime.Now}.");
                blobLeaseClient.Release();
                isCompleted = true;
                return true;

            }
            catch (RequestFailedException ex) when (ex.Status == 409) // lease already present
            {
                Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} encountered a concurrency conflict. Retrying at..{DateTime.Now}");
                attempts++;
                isCompleted=false;
                await Task.Delay(TimeSpan.FromMilliseconds(100 * attempts)); // Exponential back-off
            }
            catch (RequestFailedException ex) when (ex.Status == 412) // Precondition Failed
            {
                Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} encountered a concurrency conflict. Retrying at..{DateTime.Now}");
                attempts++;
                isCompleted=false;
                await Task.Delay(TimeSpan.FromMilliseconds(1000 * attempts)); // Exponential back-off
            }
            catch (Exception ex)
            {
                isCompleted = false;
                Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} encountered an error: {ex.Message}");
                return false;
            }
        }

        Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} failed to update after: {attempts} attempts.");
        return false;
    }

    // Example usage
    public async Task RunConcurrentUpdatesExample(bool useLock)
    {
        var partitionKey = "P001";
        var rowKey = "060321";

        // Ensure the entity exists
        //await _tableClient.UpsertEntityAsync(new TableEntity(partitionKey, rowKey) { ["Count"] = 0 });
        var sw = new Stopwatch(); 
        sw.Start();
        var tasks = new List<Task>();
        for (int i = 1; i <= 1; i++)
        {
            if (!useLock)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await UpdateEntityConcurrentlyUsingBlobLeaseClient<TableEntity>(partitionKey, rowKey, entity =>
                    {
                        entity["Count"] = (int)entity["Count"] + 2;
                        return entity;
                    });
                }));
            }
            else
            {
                tasks.Add(Task.Run(async () =>
                {
                    await UpdateEntityConcurrentlyUsingLock<TableEntity>(partitionKey, rowKey, entity =>
                    {
                        entity["Count"] = (int)entity["Count"] + 2;
                        return entity;
                    });
                }));
            }
        }
        await Task.WhenAll(tasks);

        
        var finalEntity = await _tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey);
        sw.Stop();
        Console.WriteLine($"Final count is now : {finalEntity.Value["Count"]} completed in : {sw.Elapsed.TotalSeconds}");
        Console.ReadLine();
    }
}
