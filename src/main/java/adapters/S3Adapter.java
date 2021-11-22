package adapters;


import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class S3Adapter {
    private S3Client s3;

    public S3Adapter() {
        s3 = S3Client.builder().credentialsProvider(DefaultCredentialsProvider.create()).region(Region.US_WEST_2).build();
    }

    public CreateBucketResponse createBucket(String bucket) {
        return s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(Region.US_WEST_2.id())
                                .build())
                .build());
    }

    public DeleteBucketResponse deleteBucket(String bucket) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        return s3.deleteBucket(deleteBucketRequest);
    }

    public PutObjectResponse putFileInBucketFromPath(String bucketName, String key, String filePath) throws IOException {

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        RequestBody body = RequestBody.fromFile(Paths.get(filePath));
        return s3.putObject(objectRequest, body);
    }

    public PutObjectResponse putFileInBucketFromFile(String bucketName, String key, File file) throws IOException {

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        RequestBody body = RequestBody.fromFile(file);
        return s3.putObject(objectRequest, body);
    }

    public PutObjectResponse putFileInBucketFromBytes(String bucketName, String key, byte[] file) throws IOException {

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        RequestBody body = RequestBody.fromBytes(file);
        return s3.putObject(objectRequest, body);
    }

    public void getObject(String bucketName, String key, Path destination) {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3.getObject(getObjectRequest, ResponseTransformer.toFile(destination));
    }

    public DeleteObjectResponse deleteObject(String bucketName, String key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        return s3.deleteObject(deleteObjectRequest);
    }


}
