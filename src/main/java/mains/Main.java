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
    public static final String programSqsUrl = "https://sqs.us-east-1.amazonaws.com/595412436255/programSqs";
    public static final String programBucketName = "local-app-bucket-27031995";
    private static final Integer RUNNING = 16;
    private static final String MANAGER_NODE_NAME = "ManagerNode";
    private static final EC2Adapter ec2Adapter = new EC2Adapter();
    private static final S3Adapter s3Adapter = new S3Adapter();
    private static final SQSAdapter sqsAdapter = new SQSAdapter();

    public static void main(String[] args) {
        System.setProperty("aws.accessKeyId", AWS_ACCESS_KEY_ID);
        System.setProperty("aws.secretAccessKey", AWS_SECRET_ACCESS_KEY);
        System.setProperty("aws.sessionToken", AWS_SESSION_TOKEN);

        String inputFilePath = args[0];
        String outputFilePath = args[1];
        String n = args[2];
        boolean terminationFlag = (args.length > 3) && (args[3].equals("terminate"));
        Long localAppId = System.currentTimeMillis();
        String queueUrl = "";

        if (!isManagerRunning()) {
            initiateManagerInstance();
        }

        try {

            //upload input file to s3
            String keyName = String.format("inputFile-%s.txt", localAppId);
            s3Adapter.putFileInBucketFromPath(programBucketName, keyName, "C:\\Users\\Yonathan Wolloch\\Downloads\\input-sample-1.txt"); //todo change path to args[?]

            //send a message to sqs queue with the location of the file
            String queueName = String.format("localAppQueue-%s", localAppId);
            CreateQueueResponse createQueueResponse = sqsAdapter.createQueue(queueName);
            queueUrl = createQueueResponse.queueUrl();
            String message = keyName + "," + queueUrl + "," + n;
            if (terminationFlag) message += ",terminate";
            sqsAdapter.sendMessage(programSqsUrl, message);

        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean finished = false;
        while (!finished) {
            List<Message> message = sqsAdapter.retrieveMessage(queueUrl, 1, 30);
            if (!message.isEmpty()) {
                //do something


                finished = true;
            }
        }

    }

    private static boolean isManagerRunning() {
        List<Instance> instances = ec2Adapter.describeEC2Instances();
        List<String> runningInstancesNames = instances.stream().filter(i -> (i.state().code() == RUNNING)).map(i -> i.tags().get(0).value()).collect(Collectors.toList());
        return runningInstancesNames.isEmpty() || !runningInstancesNames.contains("ManagerNode");
    }

    private static void initiateManagerInstance() {
        String userData = getRunShellCommands();
        ec2Adapter.createEC2Instance(MANAGER_NODE_NAME, userData);
    }

    private static String getRunShellCommands() {
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
        commands += "echo aws_access_key_id=" + AWS_ACCESS_KEY_ID + " >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_secret_access_key=" + AWS_SECRET_ACCESS_KEY + " >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_session_token=" + AWS_SESSION_TOKEN + " >> /home/ec2-user/.aws/credentials\n";

        commands += "aws configure set AWS_ACCESS_KEY_ID " + AWS_ACCESS_KEY_ID + "\n";
        commands += "aws configure set AWS_SECRET_ACCESS_KEY " + AWS_SECRET_ACCESS_KEY + "\n";
        commands += "aws configure set AWS_SESSION_TOKEN " + AWS_SESSION_TOKEN + "\n";
        commands += "export AWS_ACCESS_KEY_ID=" + AWS_ACCESS_KEY_ID + "\n";
        commands += "export AWS_SECRET_ACCESS_KEY=" + AWS_SECRET_ACCESS_KEY + "\n";
        commands += "export AWS_SESSION_TOKEN=" + AWS_SESSION_TOKEN + "\n";

        // get jar from s3 bucket
        commands += "aws s3 cp s3://local-app-bucket-27031995/PDF_HANDLER.jar /home/ec2-user/\n";

        // run java
        commands += "java -jar /home/ec2-user/PDF_HANDLER.jar\n";
        commands += "echo 'Woot!' > /home/ec2-user/user-script-output.txt\n";//TODO: delete
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}

