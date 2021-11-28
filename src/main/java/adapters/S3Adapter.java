package adapters;


import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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

    public BufferedReader getObject(String bucketName, String key) {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest);
        return new BufferedReader(new InputStreamReader(response));
    }

    public DeleteObjectResponse deleteObject(String bucketName, String key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        return s3.deleteObject(deleteObjectRequest);
    }


}
