import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;


public class Main {
    private static final Integer RUNNING = 16;

    public static void main(String[] args) {
        EC2Adapter ec2Adapter = new EC2Adapter();
        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();

        File f = new File("http://www.jewishfederations.org/local_includes/downloads/39497.pdf");

        //check if manager node is active
        List<Instance> instances = ec2Adapter.describeEC2Instances();

        List<String> runningInstancesNames = instances.stream().filter(i -> (i.state().code() == RUNNING)).map(i -> i.tags().get(0).value()).collect(Collectors.toList());
        if (!runningInstancesNames.contains("ManagerNode")) {
            //start the manager node
            try {
                String ec2ID = ec2Adapter.createEC2Instance("ManagerNode", Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get("C:\\Users\\Yonathan Wolloch\\IdeaProjects\\PDF_HANDLER\\src\\run.sh"))));
                System.out.println("ec2Id: " + ec2ID);


                //upload input file to s3
                String bucketName = "working-bucket" + System.currentTimeMillis();
                String keyName = "inputFile";
                s3Adapter.createBucket(bucketName);
                PutObjectResponse response = s3Adapter.putFileInBucketFromPath(bucketName, keyName, "C:\\Users\\Yonathan Wolloch\\Downloads\\input-sample-1.txt");


                //send a message to sqs queue with the location of the file
                String queueName = "ManagerQueue";
                CreateQueueResponse createQueueResponse = sqsAdapter.createQueue(queueName);
                String queueUrl = createQueueResponse.queueUrl();
                sqsAdapter.sendMessage(queueUrl, bucketName + "," + keyName);

                //check sqs for message if app is done
                List<Message> messages = sqsAdapter.retrieveMessage(queueUrl);
                System.out.println("done");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        //get results and create an html from the results

        //in case of termination mode from cli sends termination to manager node

        //String instanceId = ec2Adapter.createEC2Instance("Elad", " ");
        //CreateBucketResponse bucketResponse = s3Adapter.createBucket("");
    }
}

