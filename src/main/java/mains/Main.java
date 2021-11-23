package mains;

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
    public static final String AWS_ACCESS_KEY_ID = "ASIAYVIKYYUPUILNHXF3";
    public static final String AWS_SECRET_ACCESS_KEY = "RPU9RZyPz6pj9V6sx+IcnPFKRLsrdPN9p6Xn+/QV";
    public static final String AWS_SESSION_TOKEN = "FwoGZXIvYXdzEHwaDH04RwuQNyx9giiGqCLGAbMWyH/XKd1LWrrYXwtQ2Wy69YefMVaHBlx8bZX/YvguXSc9gMAvYlUXZ5mBml34jCkZklQfWrie0+yBA29BgvOWBHOOEuv16AVCNodLKHC1Da59oiFsl5O38i6r7pALeOJbSpjKT9MmcsHA5dxvxRrZwmNBENtdD6eu6tIKac9WkiQ+WIYS3sQsSeU9niB4QG/G+2KnmiWdBK6UkQId+nVRDcxXd8ornKkZjxTS5awbiZ/exm1JBpxWADo2Dj4IlzK0SsBStyj1xO+MBjItyWsKB1EXk+uGdnh5ukj9HISGejKdRPU1a02c9pw9zJbBAt7OYg7WF3fYmWoL";
    public static final String currDir = System.getProperty("user.dir");
    private static final Integer RUNNING = 16;
    private static final String MANAGER_NODE_NAME = "ManagerNode";
    private static String managerInstanceId = "";

    public static void main(String[] args) {
        System.setProperty("aws.accessKeyId", AWS_ACCESS_KEY_ID);
        System.setProperty("aws.secretAccessKey", AWS_SECRET_ACCESS_KEY);
        System.setProperty("aws.sessionToken", AWS_SESSION_TOKEN);
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

                //send a message to sqs queue with the location of the file
                String queueName = "ManagerQueue";
                CreateQueueResponse createQueueResponse = sqsAdapter.createQueue(queueName);
                String queueUrl = createQueueResponse.queueUrl();
                sqsAdapter.sendMessage(queueUrl, bucketName + "," + keyName);

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
        System.out.println(sqsUrl + " -- " + bucketName);
        String commands = "";
        commands += "#!/bin/bash\n";
        // install java
        commands += "sudo yum install -y java-1.8.0-openjdk\n";

        //create .aws dir
        commands += "mkdir /home/ec2-user/.aws\n";

        //create config file
        commands += "echo [default] > /home/ec2-user/.aws/config\n";
        commands += "echo region=us-east-1 >> /home/ec2-user/.aws/config\n";

        //create credentials file
        commands += "echo [default] > /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_access_key_id=ASIAYVIKYYUPUILNHXF3 >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_secret_access_key=RPU9RZyPz6pj9V6sx+IcnPFKRLsrdPN9p6Xn+/QV >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_session_token=FwoGZXIvYXdzEHwaDH04RwuQNyx9giiGqCLGAbMWyH/XKd1LWrrYXwtQ2Wy69YefMVaHBlx8bZX/YvguXSc9gMAvYlUXZ5mBml34jCkZklQfWrie0+yBA29BgvOWBHOOEuv16AVCNodLKHC1Da59oiFsl5O38i6r7pALeOJbSpjKT9MmcsHA5dxvxRrZwmNBENtdD6eu6tIKac9WkiQ+WIYS3sQsSeU9niB4QG/G+2KnmiWdBK6UkQId+nVRDcxXd8ornKkZjxTS5awbiZ/exm1JBpxWADo2Dj4IlzK0SsBStyj1xO+MBjItyWsKB1EXk+uGdnh5ukj9HISGejKdRPU1a02c9pw9zJbBAt7OYg7WF3fYmWoL >> /home/ec2-user/.aws/credentials\n";

        commands += "aws configure set AWS_ACCESS_KEY_ID " + AWS_ACCESS_KEY_ID + "\n";
        commands += "aws configure set AWS_SECRET_ACCESS_KEY " + AWS_SECRET_ACCESS_KEY + "\n";
        commands += "aws configure set AWS_SESSION_TOKEN " + AWS_SESSION_TOKEN + "\n";
        commands += "export AWS_ACCESS_KEY_ID=" + AWS_ACCESS_KEY_ID + "\n";
        commands += "export AWS_SECRET_ACCESS_KEY=" + AWS_SECRET_ACCESS_KEY + "\n";
        commands += "export AWS_SESSION_TOKEN=" + AWS_SESSION_TOKEN + "\n";

        // get jar from s3 bucket
        commands += "aws s3 cp s3://local-app-bucket-27031995/PDF_HANDLER.jar /home/ec2-user/\n";

        // run java
        commands += String.format("java -jar /home/ec2-user/PDF_HANDLER.jar %s %s %s\n", sqsUrl, 20/*args[3]*/, bucketName);
        commands += "echo 'Woot!' > /home/ec2-user/user-script-output.txt\n";
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}

