using Amazon.S3.Model;

namespace AwsUpload
{
    public class DownloadedStatFile : IDisposable
    {
        public S3Object S3Object { get; }
        public string FilePath { get; }
        public MetadataCollection Metadata { get; }

        public string Key => S3Object.Key;

        public DownloadedStatFile(S3Object s3Object, string filePath, MetadataCollection getObjectMetadataResponse)
        {
            S3Object = s3Object;
            FilePath = filePath;
            Metadata = getObjectMetadataResponse;
        }

        public void Dispose()
        {
            File.Delete(FilePath);
        }
    }
}
