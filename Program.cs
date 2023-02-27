using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.IO.Compression;
using System.Linq;

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
                    using Stream stream = await client.Stream(file);
                    using ZipArchive zipFile = new ZipArchive(stream, ZipArchiveMode.Read);

                    foreach ((long PO, string FileName) in zipFile.CSVs().First().POToFileMap())
                    {
                        ZipArchiveEntry? entry = zipFile.PDFs().FirstOrDefault(x => x.Name == FileName);

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
                else
                {
                    Debug.WriteLine($"{file.Key} already processed on {file.Metadata.ProcessedTime()}");
                }
            }
        }
    }
}
