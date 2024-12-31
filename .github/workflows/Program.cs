// See https://aka.ms/new-console-template for more information
////http://127.0.0.1:10002/devstoreaccount1

using CsvHelper;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

var account = "1001";

/*
Console.WriteLine("Running multi threaded example now...!");
//var str  = string.Format("Sync log requests syncing  rows {count}.", 2);

await new ConcurrentTableStorageManager("UseDevelopmentStorage = true", "CapRequestLogs").RunConcurrentUpdatesExample(useLock:false);
*/

//await new FileService("inventory.csv").ProcessCsvFile(); 
//await new FileService("invalidHeader.csv").ProcessCsvFile();

/*
var storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
var tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());
var table = tableClient.GetTableReference("mytable");
*/

await new ConcurrentTableStorageManager("UseDevelopmentStorage=true", "CapRequestLogs").RunConcurrentUpdatesExample(useLock: false);
Console.ReadLine();