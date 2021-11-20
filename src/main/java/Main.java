import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;

public class Main {
    public static void main(String[] args) {
        EC2Adapter ec2Adapter = new EC2Adapter();
        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();
        String instanceId = ec2Adapter.createEC2Instance("Elad", " ");
        CreateBucketResponse bucketResponse = s3Adapter.createBucket("");
        System.out.println("here");
    }
}

