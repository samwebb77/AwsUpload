using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System.Diagnostics;
using System.IO.Compression;

namespace AwsUpload
{
    public class BucketClient
    {
        private const string BucketName = "stat-coding-ayuiygcuvu2";
        private static RegionEndpoint BucketRegion => RegionEndpoint.USEast2;
        private AmazonS3Client S3Client { get; }

        public BucketClient(string accessKey, string secretKey)
        {
            var env = new BasicAWSCredentials(accessKey, secretKey);
            S3Client = new AmazonS3Client(env, BucketRegion);
        }

        public async IAsyncEnumerable<StatFile> GetFilesWhere(Predicate<S3Object> predicate)
        {
            var request = new ListObjectsV2Request
            {
                BucketName = BucketName
            };
            ListObjectsV2Response response;
            do
            {
                response = await S3Client.ListObjectsV2Async(request);

                foreach (var file in response.S3Objects.Where(s3 => predicate(s3)))
                {
                    var getObjectMetadataRequest = new GetObjectMetadataRequest() { BucketName = BucketName, Key = file.Key };

                    var metadataResponse = await S3Client.GetObjectMetadataAsync(getObjectMetadataRequest);
                    if (metadataResponse is null)
                    {
                        throw new Exception();
                    }

                    yield return new StatFile(file, metadataResponse.Metadata);

                }
            } while (!response.IsTruncated);
        }

        public async Task<DownloadedStatFile> Download(StatFile file)
        {
            var fileName = $"C:\\Stat\\{file.Key}";

            using TransferUtility fileTransferUtility = new TransferUtility(S3Client);
            await fileTransferUtility.DownloadAsync(fileName, BucketName, file.Key);

            return new DownloadedStatFile(file.S3Object, fileName, file.Metadata);
        }


        public async Task<Stream> Stream(StatFile file)
        {            
            GetObjectRequest getObjRequest = new GetObjectRequest()
            {
                BucketName = BucketName,
                Key = file.Key,
            };

            GetObjectResponse getObjRespone = await S3Client.GetObjectAsync(getObjRequest);

            return getObjRespone.ResponseStream;          
        }

        public async Task Upload(ZipArchiveEntry entry, long PO, StatFile file)
        {
            using TransferUtility fileTransferUtility = new(S3Client);

            using var fileStream = entry.Open();
            using var ms = new MemoryStream();
            fileStream.CopyTo(ms);
            ms.Position = 0;

            var request = new TransferUtilityUploadRequest()
            {
                InputStream = ms,
                BucketName = BucketName,
                Key = $"by-po/{PO}/{entry.Name}",
            };

            request.Metadata.SetProcessedTime();
            request.Metadata.SetOriginalZipFile(file);

            await fileTransferUtility.UploadAsync(request);

            Debug.WriteLine($"Uploaded {PO}/{entry.Name} from {file.Key}");
        }

        public async Task MarkProcessed(StatFile file)
        {
            CopyObjectRequest request = new CopyObjectRequest()
            {
                SourceBucket = BucketName,
                SourceKey = file.Key,
                DestinationBucket = BucketName,
                DestinationKey = file.Key,
                MetadataDirective = S3MetadataDirective.REPLACE
            };

            foreach (var key in file.Metadata.Keys)
            {
                request.Metadata.Add(key, file.Metadata[key]);
            };

            request.Metadata.SetProcessedTime();

            await S3Client.CopyObjectAsync(request);

            Debug.WriteLine($"---------------------------");
            Debug.WriteLine($"Marked {file.Key} Processed");
            Debug.WriteLine($"---------------------------");
        }
    }
}
