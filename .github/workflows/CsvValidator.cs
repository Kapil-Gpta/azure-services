using System.Globalization;
using Azure.Storage.Blobs;
using CsvHelper;

public class CsvValidator
{
    //private readonly BlobClient _blobClient;
    private readonly BlobContainerClient _blobContainer;
    private string BlobConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
    // Expected headers
    List<string> errors = new List<string>();
    private readonly string[] _expectedHeaders =
    {
        "InventoryCode", "InvDesc", "Qty"
    };

    public CsvValidator(string containerName, string blobName)
    {
        //_blobClient = new BlobClient(BlobConnectionString, containerName, blobName);
        _blobContainer = new BlobContainerClient(BlobConnectionString, "spenda-integration");
    }

    public async Task<(bool isValid, List<string> errors)> ValidateCsvFile(string blobName)
    {
        try
        {
            // Get blob
            //var containerClient = _blobContainer.GetBlobContainerClient(containerName);
            var blobClient = _blobContainer.GetBlobClient(blobName);

            // Download and read CSV
            using var memoryStream = new MemoryStream();
            await blobClient.DownloadToAsync(memoryStream);
            memoryStream.Position = 0;

            using var reader = new StreamReader(memoryStream);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

            // Read headers
            csv.Read();
            csv.ReadHeader();
            var headers = csv.HeaderRecord;

            // Validate headers
            if (headers == null)
            {
                errors.Add("No headers found in CSV file");
                return (false, errors);
            }

            // Check missing headers
            var missingHeaders = _expectedHeaders
                .Except(headers, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (missingHeaders.Any())
            {
                errors.Add($"Missing headers: {string.Join(", ", missingHeaders)}");
            }

            // Check extra headers
            var extraHeaders = headers
                .Except(_expectedHeaders, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (extraHeaders.Any())
            {
                errors.Add($"Unexpected headers: {string.Join(", ", extraHeaders)}");
            }

            // proper headers notification to the user
            if (extraHeaders.Any() || missingHeaders.Any())
                Console.WriteLine($"Headers must be: {string.Join(", ", _expectedHeaders)}");

            // Optional: Validate data rows
            int rowNumber = 1;
            while (csv.Read())
            {
                rowNumber++;

                // Example validation for each column
                try
                {
                    var code = csv.GetField(0);
                    var desc = csv.GetField(1);
                    var qty = csv.GetField<int>(2);
                    

                    if (string.IsNullOrEmpty(code))
                        errors.Add($"Row {rowNumber}: InvCode is required");

                    if (string.IsNullOrEmpty(desc))
                        errors.Add($"Row {rowNumber}: Invalid inv desc.");

                    if (qty < 0 )
                        errors.Add($"Row {rowNumber}: Qty must be +ve value");

                    Console.WriteLine($"Data is: {code},{desc},{qty}");
                }
                catch (Exception ex)
                {
                    errors.Add($"Row {rowNumber}: Invalid data format - {ex.Message}");
                }
            }

            return (!errors.Any(), errors);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex+ "Error validating CSV file");
            errors.Add($"Error processing file: {ex.Message}");
            return (false, errors);
        }
    }
}

// Usage example
public class FileService
{
    private readonly CsvValidator _validator;
    string _blobName=string.Empty;
    public FileService(string blobName)
    {
        _blobName = blobName;
        _validator = new CsvValidator("spenda-integration", _blobName);
    }

    public async Task ProcessCsvFile()
    {
        var (isValid, errors) = await _validator.ValidateCsvFile(_blobName);

        if (!isValid)
        {
            Console.WriteLine("**** Errors in file.***");
            foreach (var error in errors)
            {
                Console.WriteLine(error);
            }
        }
    }
}