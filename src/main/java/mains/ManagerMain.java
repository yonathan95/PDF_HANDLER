package mains;

import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;

public class ManagerMain {
    private static final EC2Adapter ec2Adapter = new EC2Adapter();
    private static final S3Adapter s3Adapter = new S3Adapter();
    private static final SQSAdapter sqsAdapter = new SQSAdapter();
    private static final HashMap<String, Integer> approxWorkCount = new HashMap<>();
    private static final HashMap<String, HashSet<String>> localAppsMap = new HashMap<>();
    private static final List<String> workers = new ArrayList<>();
    private static String workersInputQueueUrl;
    private static String workersOutputQueueUrl;

    public static void main(String[] args) {
        System.setProperty("aws.accessKeyId", Main.AWS_ACCESS_KEY_ID);
        System.setProperty("aws.secretAccessKey", Main.AWS_SECRET_ACCESS_KEY);
        System.setProperty("aws.sessionToken", Main.AWS_SESSION_TOKEN);
        boolean terminated = false;
        CreateQueueResponse inputQueue = sqsAdapter.createQueue("workers-input-queue");
        workersInputQueueUrl = inputQueue.queueUrl();
        CreateQueueResponse outputQueue = sqsAdapter.createQueue("workers-output-queue");
        workersOutputQueueUrl = outputQueue.queueUrl();
        List<String> toRemove = new ArrayList<>();

        while (true) {
            if (!terminated) {
                List<Message> messages = sqsAdapter.retrieveOneMessage(Main.programSqsUrl);
                if (!messages.isEmpty()) {
                    String[] messageData = messages.get(0).body().split(",");
                    String inputFileKey = messageData[0];
                    String localAppSqs = messageData[1];
                    int n = Integer.parseInt(messageData[2]);
                    localAppsMap.put(localAppSqs, new HashSet<>());
                    startLocalAppRequest(inputFileKey, localAppSqs, n);
                    sqsAdapter.deleteMessage(messages, Main.programSqsUrl);
                    if (messageData.length == 4) terminated = true;
                }
            }


            List<Message> workersMessages = sqsAdapter.retrieveMessage(workersOutputQueueUrl, 10, 1);
            for (Message message : workersMessages) {
                handleWorkerMessage(message);
            }
            sqsAdapter.deleteMessage(workersMessages, workersOutputQueueUrl);


            for (Map.Entry<String, HashSet<String>> localApp : localAppsMap.entrySet()) {
                if (approxWorkCount.containsKey(localApp.getKey()) && (localApp.getValue().size() == approxWorkCount.get(localApp.getKey()))) {
                    summarizeWork(localApp);
                    toRemove.add(localApp.getKey());
                }
            }

            for (String localAppQueue : toRemove) {
                localAppsMap.remove(localAppQueue);
                approxWorkCount.remove(localAppQueue);
            }

            if (terminated && localAppsMap.isEmpty()) {
                terminate();
                sqsAdapter.removeQueue(workersInputQueueUrl);
                sqsAdapter.removeQueue(workersOutputQueueUrl);
                selfDestruct();
                break;
            }
        }
    }

    private static void selfDestruct() {
        List<Instance> instances = ec2Adapter.describeEC2Instances();
        instances.stream().filter(i -> (i.state().code().equals(Main.RUNNING)) && (i.tags().get(0).value().equals("ManagerNode"))).map(Instance::instanceId).forEach(ec2Adapter::terminateEC2Instance);
    }


    private static void summarizeWork(Map.Entry<String, HashSet<String>> localApp) {
        StringBuilder summary = new StringBuilder();
        for (String line : localApp.getValue()) {
            summary.append(line).append("\n");
        }
        String key = String.format("summary-%s", currentTimeMillis());
        try {
            s3Adapter.putFileInBucketFromBytes(Main.programBucketName, key, summary.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        sqsAdapter.sendMessage(localApp.getKey(), key);
    }

    private static void handleWorkerMessage(Message message) {
        String[] workerMessageData = message.body().split(",");
        String localAppUrl;
        String origUrl;
        String reason;
        String action;
        if ("success".equals(workerMessageData[4])) {
            origUrl = workerMessageData[0];
            String key = workerMessageData[1];
            localAppUrl = workerMessageData[2];
            action = workerMessageData[3];
            reason = String.format("https://%s.s3.us-west-2.amazonaws.com/%s", Main.programBucketName, key);
        } else {
            action = workerMessageData[0];
            origUrl = workerMessageData[1];
            reason = workerMessageData[2];
            localAppUrl = workerMessageData[3];
        }
        if (localAppsMap.containsKey(localAppUrl)) {
            localAppsMap.get(localAppUrl).add(action + "\t" + origUrl + "\t" + reason + "\n");
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
        HashSet<String> linesSet = new HashSet<>();
        for (String line : lines) {
            String[] data = line.split("\t");
            String url = data[1];
            String action = data[0];
            String message = url + "," + action + "," + localAppSqs;
            linesSet.add(line);
            sqsAdapter.sendMessage(workersInputQueueUrl, message);
        }
        approxWorkCount.put(localAppSqs, linesSet.size());

        //create workers
        List<Instance> openInstances = ec2Adapter.describeEC2Instances().stream().filter(i -> i.state().code() != 48).collect(Collectors.toList());
        int maxNumberOfWorkers = 9 - openInstances.size();
        for (int i = 0; i < Math.min(maxNumberOfWorkers, Math.ceil(lines.size() / n)); i++) {
            String userData = getRunShellCommands(workersInputQueueUrl, workersOutputQueueUrl);
            String workerId = ec2Adapter.createEC2Instance(String.format("worker-%s", i), userData, InstanceType.T2_MICRO);
            workers.add(workerId);
        }
    }

    private static String getRunShellCommands(String inputSqsUrl, String outputSqsUrl) {
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
        commands += String.format("java -jar /home/ec2-user/PDF_HANDLER_WORKER.jar %s %s %s\n", inputSqsUrl, outputSqsUrl, Main.programBucketName);
        commands += "echo 'Woot!' > /home/ec2-user/user-script-output.txt\n";
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}


