package mains;

import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class ManagerMain {
    private static final EC2Adapter ec2Adapter = new EC2Adapter();
    private static final S3Adapter s3Adapter = new S3Adapter();
    private static final SQSAdapter sqsAdapter = new SQSAdapter();
    private static String workersInputQueueUrl;
    private static String workersOutputQueueUrl;
    private static HashMap<String, Integer> approxWorkCount = new HashMap<>();
    private static HashMap<String, HashSet<String>> localAppsMap = new HashMap<>();
    private static List<String> workers = new ArrayList<>();

    public static void main(String[] args) throws FileNotFoundException { //params 0: local-app sqsUrl, params 1: n, params 2: bucket name
        System.setProperty("aws.accessKeyId", Main.AWS_ACCESS_KEY_ID);
        System.setProperty("aws.secretAccessKey", Main.AWS_SECRET_ACCESS_KEY);
        System.setProperty("aws.sessionToken", Main.AWS_SESSION_TOKEN);
        boolean terminated = false;
        CreateQueueResponse inputQueue = sqsAdapter.createQueue("workers-input-queue");
        workersInputQueueUrl = inputQueue.queueUrl();
        CreateQueueResponse outputQueue = sqsAdapter.createQueue("workers-output-queue");
        workersOutputQueueUrl = outputQueue.queueUrl();


        while (true) {
            //listen to new work
            if (!terminated) {
                List<Message> messages = sqsAdapter.retrieveOneMessage(Main.programSqsUrl);
                if (!messages.isEmpty()) {
                    //handle new job
                    String[] messageData = messages.get(0).body().split(",");
                    String inputFileKey = messageData[0];
                    String localAppSqs = messageData[1];
                    int n = Integer.parseInt(messageData[2]);
                    if (messageData.length == 4) terminated = true;
                    localAppsMap.put(localAppSqs, new HashSet<>());
                    startLocalAppRequest(inputFileKey, localAppSqs, n);
                    sqsAdapter.deleteMessage(messages, Main.programSqsUrl);
                }
            }

            List<Message> workersMessages = sqsAdapter.retrieveMessage(workersOutputQueueUrl, 10, 1);
            for (Message message : workersMessages) {
                //add implementation
                handleWorkerMessage(message);
            }
            sqsAdapter.deleteMessage(workersMessages, workersOutputQueueUrl);


            for (Map.Entry<String, HashSet<String>> localApp : localAppsMap.entrySet()) {
                if (localApp.getValue().size() == approxWorkCount.get(localApp.getKey())) {
                    summarizeWork(localApp);
                    localAppsMap.remove(localApp.getKey());
                    approxWorkCount.remove(localApp.getKey());
                }
            }

            if (terminated && approxWorkCount.isEmpty()) {
                //terminate();
                break;
            }
        }
    }

    private static void summarizeWork(Map.Entry<String, HashSet<String>> localApp) {
        String summary = "";
        for (String line : localApp.getValue()) {
            summary += line + "\n";
        }
        String key = String.format("summary-%s", "1");
        try {
            s3Adapter.putFileInBucketFromBytes(Main.programBucketName, key, summary.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        sqsAdapter.sendMessage(localApp.getKey(), key);
    }

    private static void handleWorkerMessage(Message message) {
        String[] workerMessageData = message.body().split(",");
        switch (workerMessageData[4]) {
            case "success": {
                String origUrl = workerMessageData[0];
                String key = workerMessageData[1];
                String localAppUrl = workerMessageData[2];
                String action = workerMessageData[3];
                String newUrl = String.format("https://%s.s3.us-west-2.amazonaws.com/%s", Main.programBucketName, key);
                localAppsMap.get(localAppUrl).add(action + "\t" + origUrl + "\t" + newUrl + "\n");
                break;
            }
            default: {
                String action = workerMessageData[0];
                String origUrl = workerMessageData[1];
                String whyFail = workerMessageData[2];
                String localAppUrl = workerMessageData[3];
                localAppsMap.get(localAppUrl).add(action + "\t" + origUrl + "\t" + whyFail + "\n");
                break;
            }
        }
    }

    private static void terminate() {
        for (String worker : workers) {
            try {
                ec2Adapter.terminateEC2Instance(worker);
            } catch (Exception ignored) {
            }
        }
    }

    private static void startLocalAppRequest(String inputFileKey, String localAppSqs, int n) {
        BufferedReader buffer = s3Adapter.getObject(Main.programBucketName, inputFileKey);
        List<String> lines = buffer.lines().collect(Collectors.toList());
        approxWorkCount.put(localAppSqs, lines.size());

        for (String line : lines) {
            String[] data = line.split("\t");
            String url = data[1];
            String action = data[0];
            String message = url + "," + action + "," + localAppSqs;
            sqsAdapter.sendMessage(workersInputQueueUrl, message);
        }

        //create workers
        List<Instance> openInstances = ec2Adapter.describeEC2Instances().stream().filter(i -> i.state().code().intValue() != 43).collect(Collectors.toList());
        int maxNumberOfWorkers = 10 - openInstances.size();
        for (int i = 0; i < Math.min(maxNumberOfWorkers, Math.ceil(lines.size() / n)); i++) {
            System.out.println(workersInputQueueUrl + " " + workersOutputQueueUrl + " " + Main.programBucketName);
            String userData = getRunShellCommands(workersInputQueueUrl, workersOutputQueueUrl, Main.programBucketName);
            String workerId = ec2Adapter.createEC2Instance(String.format("worker-%s", i), userData);
            workers.add(workerId);
        }
    }

    private static String getRunShellCommands(String inputSqsUrl, String outputSqsUrl, String bucketName) {
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
        commands += "echo aws_access_key_id=" + Main.AWS_ACCESS_KEY_ID + " >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_secret_access_key=" + Main.AWS_SECRET_ACCESS_KEY + " >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_session_token=" + Main.AWS_SESSION_TOKEN + " >> /home/ec2-user/.aws/credentials\n";

        commands += "aws configure set AWS_ACCESS_KEY_ID " + Main.AWS_ACCESS_KEY_ID + "\n";
        commands += "aws configure set AWS_SECRET_ACCESS_KEY " + Main.AWS_SECRET_ACCESS_KEY + "\n";
        commands += "aws configure set AWS_SESSION_TOKEN " + Main.AWS_SESSION_TOKEN + "\n";
        commands += "export AWS_ACCESS_KEY_ID=" + Main.AWS_ACCESS_KEY_ID + "\n";
        commands += "export AWS_SECRET_ACCESS_KEY=" + Main.AWS_SECRET_ACCESS_KEY + "\n";
        commands += "export AWS_SESSION_TOKEN=" + Main.AWS_SESSION_TOKEN + "\n";

        // get jar from s3 bucket
        commands += "aws s3 cp s3://program-bucket-28031995/PDF_HANDLER_WORKER.jar /home/ec2-user/\n";
        // run java
        commands += String.format("java -jar /home/ec2-user/PDF_HANDLER_WORKER.jar %s %s %s\n", inputSqsUrl, outputSqsUrl, bucketName);
        commands += "echo 'Woot!' > /home/ec2-user/user-script-output.txt\n";
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}


