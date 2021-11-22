import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Scanner;

public class ManagerMain {
    public static void main(String[] args) throws FileNotFoundException { //params 1: local-app sqsUrl, params 2: n, params 3: bucket name
        EC2Adapter ec2Adapter = new EC2Adapter();
        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();

        //get the input file from s3
        List<Message> messages = sqsAdapter.retrieveMessage(args[1]);
        String inputFile = messages.get(0).body();
        File file = new File(inputFile);

        //create workers queue
        CreateQueueResponse createQueueResponse = sqsAdapter.createQueue("workers-queue");
        String inputQueueUrl = createQueueResponse.queueUrl();
        String outputQueueUrl = createQueueResponse.queueUrl();

        //create sqs message for each URL in the input file and add it to the sqs
        int fileCount = 0;
        Scanner myReader = new Scanner(file);
        while (myReader.hasNextLine()) {
            String line = myReader.nextLine();
            String[] data = line.split("\t");
            String url = data[1];
            String action = data[0];
            String message = url + "," + action;
            sqsAdapter.sendMessage(inputQueueUrl, message);
            fileCount += 1;
        }
        myReader.close();

        //create workers
        List<String> workers = new ArrayList<String>();
        for (int i = 0; i < Math.min(19, Math.ceil(fileCount / Integer.parseInt(args[2]))); i++) {
            String userData = getRunShellCommands(inputQueueUrl, outputQueueUrl, args[3]);
            String workerId = ec2Adapter.createEC2Instance(String.format("worker-%s", i), userData);
            workers.add(workerId);
        }
        //termination
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (String workerId : workers) {
                    ec2Adapter.terminateEC2Instance(workerId);
                }
            }
        });
    }

    private static String getRunShellCommands(String inputSqsUrl, String outputSqsUrl, String bucketName) {
        String commands = "";
        commands += "#!/bin/bash\n";
        // install java
        commands += "sudo yum install -y java-1.8.0-openjdk\n";

        //define env variables
        commands += "aws configure set AWS_ACCESS_KEY_ID " + Main.AWS_ACCESS_KEY_ID + "\n";
        commands += "aws configure set AWS_SECRET_ACCESS_KEY " + Main.AWS_SECRET_ACCESS_KEY + "\n";
        commands += "aws configure set AWS_SESSION_TOKEN " + Main.AWS_SESSION_TOKEN + "\n";
        commands += "aws configure set region us-west-2\n";

        // get jar from s3 bucket
        commands += "aws s3 cp s3://local-app-bucket-27031995/PDF_HANDLER_WORKER.jar .\n";

        // run java
        commands += String.format("java -jar PDF_HANDLER_WORKER.jar %s %s %s\n", inputSqsUrl, outputSqsUrl, bucketName);
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}
