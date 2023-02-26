using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.IO.Compression;

namespace AwsUpload
{
    public static class ZipExtensions
    {
        public static IEnumerable<ZipArchiveEntry> CSVs(this ZipArchive zipArchive)
        {
            return zipArchive.Entries.Where(x => x.FullName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase));
        }

        public static IEnumerable<ZipArchiveEntry> PDFs(this ZipArchive zipArchive)
        {
            return zipArchive.Entries.Where(x => x.FullName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase));
        }

        public static IEnumerable<(long PONumber, string FileName)> POToFileMap(this ZipArchiveEntry entry)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = "~",
            };

            using (var reader = new StreamReader(entry.Open()))
            using (var csv = new CsvReader(reader, config))
            {
                csv.Read();
                csv.ReadHeader();
                while (csv.Read())
                {
                    foreach (var y in csv.GetField<string>("Attachment List").Split(',').Select(x => x.Split("/").Last()))
                    {
                        yield return (csv.GetField<long>("PO Number"), y);
                    }
                }
            }
        }
    }
}
