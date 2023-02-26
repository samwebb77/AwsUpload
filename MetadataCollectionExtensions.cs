using Amazon.S3.Model;

namespace AwsUpload
{
    public static class MetadataCollectionExtensions
    {
        private const string ProcessedMetaDataKey = "SamCodeProcessedOn";
        private const string OriginZipFileMetaDataKey = "OrginZipFile";

        public static bool IsProcessed(this MetadataCollection metadataCollection) => metadataCollection.Keys.Contains($"x-amz-meta-{ProcessedMetaDataKey}");
        public static void SetProcessedTime(this MetadataCollection metadataCollection)
        {
            metadataCollection.Add(ProcessedMetaDataKey, DateTimeOffset.UtcNow.ToString());
        }

        public static void SetOriginalZipFile(this MetadataCollection metadataCollection, StatFile file)
        {
            metadataCollection.Add(OriginZipFileMetaDataKey, file.Key);
        }
    }
}
