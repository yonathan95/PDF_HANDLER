package adapters;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class SQSAdapter {
    private SqsClient sqsClient;

    public SQSAdapter() {
        sqsClient = SqsClient.builder().region(Region.US_EAST_1).credentialsProvider(DefaultCredentialsProvider.create()).build();
    }


    public void deleteMessage(List<Message> messages, String queueUrl) {
        for (Message message : messages) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        }
    }

    public CreateQueueResponse createQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        return sqsClient.createQueue(createQueueRequest);
    }

    public SendMessageResponse sendMessage(String queueUrl, String body) {
        return sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(body)
                .build());
    }

    public List<Message> retrieveMessage(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(5)
                .build();
        return sqsClient.receiveMessage(receiveMessageRequest).messages();
    }

    public List<Message> retrieveOneMessage(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();
        return sqsClient.receiveMessage(receiveMessageRequest).messages();
    }

}
