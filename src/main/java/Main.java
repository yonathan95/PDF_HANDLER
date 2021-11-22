import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;


public class Main {
    public static final String AWS_ACCESS_KEY_ID = "ASIAYVIKYYUP57EV34JZ";
    public static final String AWS_SECRET_ACCESS_KEY = "OqlGAUXHd5dHSI7uVcUWO+7HgNdhmB7jYYO4Zqf+";
    public static final String AWS_SESSION_TOKEN = "FwoGZXIvYXdzEF8aDKN6H9VcRjgwf3y+7SLGAcwEmJrsFKvwPN0rFv0vyfz9FJLG/Hj5v1ycqP9Y+eqXRlomg2YCE8d6Va5jf46VHfpSc0fUv58i6AWEzhzLSvLxw9B7ZQKhCxt/pNsC/SdOQd4QDL8JmDCVkDQA9buM1N/4Yz+03PNfh/Y2dpnXxJdxzpXkHP7iIL56Uu1JE+D5MsvxOQ5yFDGl8pb3PK0PZTchr+ANiEpNhZAw/B4Yjrr397ox+zmhlSIEf4M9VPunChebEKh+2YmtcK13AUaKb1aW/yXKriiomumMBjItuDPvzAbLjaZ0WuGy+vbs43g+dsNqqhBVCNpz+DLOLyAF5JM5GrYnvZn99SsD";
    public static final String S3filePathFormat = "https://%s.s3.us-west-2.amazonaws.com/%s";
    private static final Integer RUNNING = 16;
    private static final String MANAGER_NODE_NAME = "ManagerNode";
    private static String managerInstanceId = "";

    public static void main(String[] args) {
        EC2Adapter ec2Adapter = new EC2Adapter();
        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();


        //File f = new File("https://working-bucket1637422184418.s3.us-west-2.amazonaws.com/inputFile.txt");
        //check if manager node is active
        List<Instance> instances = ec2Adapter.describeEC2Instances();

        List<String> runningInstancesNames = instances.stream().filter(i -> (i.state().code() == RUNNING)).map(i -> i.tags().get(0).value()).collect(Collectors.toList());
        if (runningInstancesNames.isEmpty() || !runningInstancesNames.contains("ManagerNode")) {

            try {

                //upload input file to s3
                String bucketName = "local-app-bucket" + System.currentTimeMillis();
                String keyName = "inputFile.txt";
                s3Adapter.createBucket(bucketName);
                s3Adapter.putFileInBucketFromPath(bucketName, keyName, "C:\\Users\\Yonathan Wolloch\\Downloads\\input-sample-1.txt"); //todo change path to args[?]
                String s3inputFilePath = String.format(S3filePathFormat, bucketName, keyName);

                //send a message to sqs queue with the location of the file
                String queueName = "ManagerQueue";
                CreateQueueResponse createQueueResponse = sqsAdapter.createQueue(queueName);
                String queueUrl = createQueueResponse.queueUrl();
                sqsAdapter.sendMessage(queueUrl, s3inputFilePath);

                //start the manager node
                String userData = getRunShellCommands(queueUrl, bucketName);
                managerInstanceId = ec2Adapter.createEC2Instance(MANAGER_NODE_NAME, userData);
                System.out.println("ec2Id: " + managerInstanceId);

                //check sqs for message if app is done
                List<Message> messages = sqsAdapter.retrieveMessage(queueUrl);

                System.out.println("done");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            managerInstanceId = instances.stream().filter(i -> (i.state().code().equals(RUNNING)) && (i.tags().get(0).value().equals(MANAGER_NODE_NAME))).map(Instance::instanceId).collect(Collectors.toList()).get(0);
        }

        //in case of termination mode from cli sends termination to manager node
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ec2Adapter.terminateEC2Instance(managerInstanceId);
            }
        });
        while (true) {

        }

        //get results and create an html from the results
    }

    private static String getRunShellCommands(String sqsUrl, String bucketName) {
        String commands = "";
        commands += "#!/bin/bash\n";
        // install java
        commands += "sudo yum install -y java-1.8.0-openjdk\n";

        //define env variables
        commands += "aws configure set AWS_ACCESS_KEY_ID " + AWS_ACCESS_KEY_ID + "\n";
        commands += "aws configure set AWS_SECRET_ACCESS_KEY " + AWS_SECRET_ACCESS_KEY + "\n";
        commands += "aws configure set AWS_SESSION_TOKEN " + AWS_SESSION_TOKEN + "\n";
        commands += "aws configure set region us-west-2\n";

        // get jar from s3 bucket
        commands += "aws s3 cp s3://local-app-bucket-27031995/PDF_HANDLER.jar .\n";

        // run java
        commands += String.format("java -jar PDF_HANDLER.jar %s %s %s\n", sqsUrl, 20/*args[3]*/, bucketName);
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}

