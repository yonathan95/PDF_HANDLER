package mains;

import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;


public class Main {
    public static final String AWS_ACCESS_KEY_ID = "ASIA4AZVGOVYBNNINVM5";
    public static final String AWS_SECRET_ACCESS_KEY = "4XsqmWw24CYTG1CLZpsMjVzqm7XjrABEaOBdzCXb";
    public static final String AWS_SESSION_TOKEN = "FwoGZXIvYXdzEJD//////////wEaDGFtFa0UudJIzdQz3yLIAbNmjRU/9jhL86BnDA/+75FfAvlNmJyUAOBc2c+26AODnslDZ/8eCRHQZxAtXcHmzAG7WAsqFnhrBLbvCyj0ah/CH/rdSbHfFYfcguCUtQfNlC0KS17vxl9vdkrlGscP5STTN74K2CxNAvRf/2X6AphCk2izAWKjSOMcTRLsc2Hy1gL9m1kl4JgCOI9xXAnX5RkXuAugMe9zX1h5cqrcQC7NMwLspjwC1HBJbhXaU7m/YPqne8G8iuOcpPTqZdfFqewbqDYZIb5VKNujrI0GMi1cfEvCe6BDn6jiD0Vy3dE+MtEWX5seGxdbxkketfx/9o9XiXYF/pD+7pOPBA8=";
    public static final String programSqsUrl = "https://sqs.us-east-1.amazonaws.com/826355905904/program-queue";
    public static final String programBucketName = "program-bucket-28031995";
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
            s3Adapter.putFileInBucketFromPath(programBucketName, keyName, inputFilePath); //todo change path to args[?]

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
            List<Message> messages = sqsAdapter.retrieveMessage(queueUrl, 1, 20);
            StringBuilder summary = new StringBuilder();
            summary.append("<html>");
            if (!messages.isEmpty()) {
                BufferedReader buffer = s3Adapter.getObject(Main.programBucketName, messages.get(0).body());
                List<String> lines = buffer.lines().collect(Collectors.toList());
                File outputFile = new File(outputFilePath);
                try {
                    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
                    for (String line : lines) {
                        summary.append("<dif><p>" + line + "</p></dif>");
                    }
                    summary.append("</html>");
                    bw.write(summary.toString());
                    bw.close();
                    finished = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static boolean isManagerRunning() {
        List<Instance> instances = ec2Adapter.describeEC2Instances();
        List<String> runningInstancesNames = instances.stream().filter(i -> (i.state().code() == RUNNING)).map(i -> i.tags().get(0).value()).collect(Collectors.toList());
        return !runningInstancesNames.isEmpty() && runningInstancesNames.contains("ManagerNode");
    }

    private static void initiateManagerInstance() {
        String userData = getRunShellCommands();
        ec2Adapter.createEC2Instance(MANAGER_NODE_NAME, userData, InstanceType.T3_MEDIUM);
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
        commands += "aws s3 cp s3://program-bucket-28031995/PDF_HANDLER_MANAGER.jar /home/ec2-user/\n";

        // run java
        commands += "java -jar /home/ec2-user/PDF_HANDLER_MANAGER.jar\n";
        commands += "echo 'Woot!' > /home/ec2-user/user-script-output.txt\n";//TODO: delete
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}

