using Amazon.S3.Model;

namespace AwsUpload
{
    public class StatFile
    {
        public StatFile(S3Object s3Object, MetadataCollection metadata)
        {
            S3Object = s3Object;
            Metadata = metadata;
        }

        public S3Object S3Object { get; }
        public MetadataCollection Metadata { get; }
        public string Key => S3Object.Key;
    }
}
