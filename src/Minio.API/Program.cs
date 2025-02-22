using Minio;
using Minio.DataModel.Args;

var builder = WebApplication.CreateBuilder(args);

var services = builder.Services;
services.AddEndpointsApiExplorer();
services.AddSwaggerGen();
services.AddMinio(client =>
{
    const string endpoint = "localhost:9000";
    const string accessKey = "WgSJKRLEc80zDibUAEvs";
    const string secretKey = "fhE7ONz89oS2nHaUFDa9CwnDg9oswUxNpEUQ5aTe";
    client.WithEndpoint(endpoint);
    client.WithCredentials(accessKey, secretKey);
    client.WithSSL(false);
    client.Build();
});

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();
app.UseHttpsRedirection();

// TODO: Bucket operationss
var bucketGroup = app.MapGroup("/api/bucket").WithTags("Bucket");
bucketGroup.MapGet("/list", async (IMinioClient minio) =>
{
    var list = await minio.ListBucketsAsync();
    return list;
});
bucketGroup.MapGet("/exists", async (IMinioClient minio, string bucketName) =>
{
    var args = new BucketExistsArgs()
        .WithBucket(bucketName);
    var result = await minio.BucketExistsAsync(args);
    return result;
});
bucketGroup.MapPost("/make", async (IMinioClient minio, string bucketName) =>
{
    var args = new MakeBucketArgs()
        .WithBucket(bucketName);
    await minio.MakeBucketAsync(args);
});
bucketGroup.MapDelete("/remove", async (IMinioClient minio, string bucketName) =>
{
    var args = new RemoveBucketArgs()
        .WithBucket(bucketName);
    await minio.RemoveBucketAsync(args);
});
bucketGroup.MapGet("/object/list", (IMinioClient minio, string bucketName, string? prefix, bool? recursive, bool? versions) =>
{
    var args = new ListObjectsArgs()
        .WithBucket(bucketName)
        .WithPrefix(prefix)
        .WithRecursive(recursive ?? true)
        .WithVersions(versions ?? false);
    return minio.ListObjectsEnumAsync(args);
});
bucketGroup.MapGet("/object/list-incomplete", (IMinioClient minio, string bucketName, string? prefix, bool? recursive, bool? versions) =>
{
    var args = new ListIncompleteUploadsArgs()
        .WithBucket(bucketName)
        .WithPrefix(prefix)
        .WithRecursive(recursive ?? true);
    return minio.ListIncompleteUploadsEnumAsync(args);
});

// TODO: Object operations
var objectGroup = app.MapGroup("/api/object").WithTags("Object");
objectGroup.MapGet("stat", async (IMinioClient minio, string bucketName, string objectName) =>
{
    var args = new StatObjectArgs()
        .WithBucket(bucketName)
        .WithObject(objectName);
    return await minio.StatObjectAsync(args);
});
objectGroup.MapGet("get", async (IMinioClient minio, string bucketName, string objectName) =>
{
    var stream = new MemoryStream();
    var args = new GetObjectArgs()
        .WithBucket(bucketName)
        .WithObject(objectName)
        .WithCallbackStream(callback =>
        {
            callback.Position = 0;
            callback.CopyTo(stream);
        })
        .WithFile(objectName);
    _ = await minio.GetObjectAsync(args);
    return stream;
});
objectGroup.MapGet("presigned", async (IMinioClient minio, string bucketName, string objectName) =>
{
    var args = new PresignedGetObjectArgs()
        .WithBucket(bucketName)
        .WithObject(objectName)
        .WithExpiry(1000);
    return await minio.PresignedGetObjectAsync(args);
});
objectGroup.MapPost("/put", async (IMinioClient minio, string bucketName, IFormFile file) => 
{
    using var stream = new MemoryStream();
    await file.CopyToAsync(stream);
    stream.Position = 0;
    var args = new PutObjectArgs()
        .WithBucket(bucketName)
        .WithObject(file.FileName)
        .WithStreamData(stream)
        .WithObjectSize(stream.Length)
        .WithContentType(file.ContentType);
    await minio.PutObjectAsync(args);
}).DisableAntiforgery();
objectGroup.MapDelete("remove", async (IMinioClient minio, string bucketName, string objectName) =>
{
    var args = new RemoveObjectArgs()
        .WithBucket(bucketName)
        .WithObject(objectName);
    await minio.RemoveObjectAsync(args);
});

app.Run();