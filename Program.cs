
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

            await foreach (var file in client.GetFilesWhere(s => s.Key.EndsWith(".zip")))
            {
                if (!file.Metadata.IsProcessed())
                {
                    using var download = await client.Download(file);

                    using ZipArchive archive = ZipFile.OpenRead(download.FilePath);

                    foreach ((long PO, string FileName) in archive.CSVs().SelectMany(csv => csv.POToFileMap()))
                    {
                        var entry = archive.PDFs().FirstOrDefault(x => x.Name == FileName);

                        if (entry != null)
                        {
                            await client.Upload(entry, PO, download);
                        }
                        else
                        {
                            Console.WriteLine($"Failed to find file {FileName} for PO {PO}");
                        }
                    }

                    await client.MarkProcessed(download);
                }
            }
        }
    }
}
