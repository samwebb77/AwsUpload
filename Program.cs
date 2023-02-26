using Microsoft.Extensions.Configuration;
using System.IO.Compression;

namespace AwsUpload
{
    class Program
    {
        public static async Task Main()
        {
            var config = new ConfigurationBuilder().AddUserSecrets<Program>().Build();

            var client = new BucketClient(config["AccessKey"], config["SecretKey"]);

            await foreach (StatFile file in client.GetFilesWhere(s => s.Key.EndsWith(".zip")))
            {
                if (!file.Metadata.IsProcessed())
                {                   
                    using var stream = await client.Stream(file);

                    using ZipArchive archive = new ZipArchive(stream, ZipArchiveMode.Read);

                    foreach ((long PO, string FileName) in archive.CSVs().SelectMany(csv => csv.POToFileMap()))
                    {
                        var entry = archive.PDFs().FirstOrDefault(x => x.Name == FileName);

                        if (entry != null)
                        {
                            await client.Upload(entry, PO, file);
                        }
                        else
                        {
                            Console.WriteLine($"Failed to find file {FileName} for PO {PO}");
                        }
                    }

                    await client.MarkProcessed(file);
                }
            }
        }
    }
}
