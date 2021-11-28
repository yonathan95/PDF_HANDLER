package mains;

import adapters.EC2Adapter;
import adapters.S3Adapter;
import adapters.SQSAdapter;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Scanner;

public class ManagerMain {
    public static void main(String[] args) throws FileNotFoundException { //params 0: local-app sqsUrl, params 1: n, params 2: bucket name
        EC2Adapter ec2Adapter = new EC2Adapter();
        S3Adapter s3Adapter = new S3Adapter();
        SQSAdapter sqsAdapter = new SQSAdapter();
        String sqsUrl = args[0];
        Integer n = Integer.parseInt(args[1]);
        String bucketName = args[2];

        //get the input file from s3
        List<Message> messages = sqsAdapter.retrieveOneMessage(sqsUrl);
        String[] messsageData = messages.get(0).body().split(",");
        String inputFilePath = Main.currDir + "/inputFile.txt";
        s3Adapter.getObject(messsageData[0], messsageData[1], Paths.get(inputFilePath));

        //create workers queue
        CreateQueueResponse inputQueue = sqsAdapter.createQueue("workers-input-queue");
        CreateQueueResponse outputQueue = sqsAdapter.createQueue("workers-output-queue");
        String inputQueueUrl = inputQueue.queueUrl();
        String outputQueueUrl = outputQueue.queueUrl();

        //create sqs message for each URL in the input file and add it to the sqs
        int fileCount = 0;
        File inputFile = new File(inputFilePath);
        Scanner myReader = new Scanner(inputFile);
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
        inputFile.delete();

        //create workers
        List<String> workers = new ArrayList<>();
        for (int i = 0; i < Math.min(7, Math.ceil(fileCount / n)); i++) {
            String userData = getRunShellCommands(inputQueueUrl, outputQueueUrl, bucketName);
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
        int count = 0;
        File summaryFile = new File("<where>\\<name>.html"); //todo - we need to decide where to save it and how to call it
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(summaryFile));
            while (count < fileCount) {

                List<Message> responseMessages = sqsAdapter.retrieveOneMessage(outputQueue.queueUrl());
                String[] response = responseMessages.get(0).body().split(",");
                if (response.length == 4) {
                    String origUrl = response[0];
                    String key = response[1];
                    String bucket = response[2];
                    String action = response[3];
                    String newUrl = String.format("https://%s.s3.us-west-2.amazonaws.com/%s", bucket, key);
                    bw.write("<dif><p>" + action + "\t" + origUrl + "\t" + newUrl + "\n</p></dif>");


                } else {
                    String action = response[0];
                    String origUrl = response[1];
                    String whyFail = response[2];
                    bw.write("<dif><p>" + action + "\t" + origUrl + "\t" + whyFail + "\n</p></dif>");
                }
                count++;
            }
            bw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
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
        commands += "echo aws_access_key_id=ASIAYVIKYYUPXEAS2NWE >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_secret_access_key=/P2nmHqtx6J/MuWuSph/gAU3HKdZZ+NztdzsoyFH >> /home/ec2-user/.aws/credentials\n";
        commands += "echo aws_session_token=FwoGZXIvYXdzEHgaDAUlc64uoRx75t547SLGAa2Z5KaIf/nMXw9WrFgiTlL0sV9xKiH2YyMVSZtRaqvlcXiK7+etZfyJttFrcrKPHYWn/jlVcQqEcVa/MGU40G1eDIpkHk5Eg7SQOgo8pD0xviyzLcqQ77cmaY0E8dZExRwJHxCcQp+1mJ641sAPi4pAViV6xxakEMj7iwtA9WHEhPMD4k1RSVdkjD9qpGkOWVe2hLKm+tlV2pMtGQFzWBO7LuSG85FVxAvAL3WrfZ5rKgwUGN4EtOx+Vv4ZBpCek9ZOvzxlTCiS0+6MBjItTdQrHJ3UWxhhzfFO+j674pxIUIxmcbpuxy6IciWlJhtn9PqUQNS5nHnPgnSJ >> /home/ec2-user/.aws/credentials\n";

        // get jar from s3 bucket
        commands += "cd /home/ec2-user\n";
        commands += "aws s3 cp s3://local-app-bucket-27031995/PDF_HANDLER_WORKER.jar .\n";
        // run java
        commands += String.format("java -jar PDF_HANDLER_WORKER.jar %s %s %s\n", inputSqsUrl, outputSqsUrl, bucketName);
        commands += "echo 'Woot!' > /home/ec2-user/user-script-output.txt\n";
        return Base64.getEncoder().encodeToString(commands.getBytes(StandardCharsets.UTF_8));
    }
}
