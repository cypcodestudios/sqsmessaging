package com.cypcode.sqsmessaging.util;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

@Service
public class AWSSQSUtil {
	
	@Value( "${aws.access.key}" )
	private String AWS_ACCESS_KEY;
	@Value("${aws.secret.key}")
	private String AWS_SECRET_KEY;

	@Value("${aws.sqs.queue}")
	private String AWS_SQS_QUEUE;
	@Value("${aws.sqs.arn}")
	private String AWS_SQS_QUEUE_ARN;
	@Value("${aws.sqs.queue.url}")
	private String AWS_SQS_QUEUE_URL;

	@Value("${aws.sqs.dl.queue}")
	private String AWS_SQS_DL_QUEUE;
	@Value("${aws.sqs.dl.arn}")
	private String AWS_SQS_QUEUE_DL_ARN;
	@Value("${aws.sqs.dl.queue.url}")
	private String AWS_SQS_QUEUE_DL_URL;
	
	private AWSCredentials awsCredentials() {
		AWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);
		return credentials;
	}

	private AmazonSQS sqsClientBuilder() {
		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials())).withRegion(Regions.SA_EAST_1)
				.build();
		return sqs;
	}

	public void createSQSQueue(String queueName) {
		AmazonSQS sqsClient = sqsClientBuilder();
		CreateQueueRequest createStandardQueueRequest = new CreateQueueRequest(queueName);
		String standardQueueUrl = sqsClient.createQueue(createStandardQueueRequest).getQueueUrl();
		System.out.println("AWS SQS Queue URL: " + standardQueueUrl);
	}

	public String produceMessageToSQS(String message) {
		AmazonSQS sqsClient = sqsClientBuilder();
		SendMessageRequest request = new SendMessageRequest().withQueueUrl(AWS_SQS_QUEUE_URL).withMessageBody(message)
				.withDelaySeconds(1);

		return sqsClient.sendMessage(request).getMessageId();
	}

	public List<Message> consumeMessageFromSQS() {
		AmazonSQS sqsClient = sqsClientBuilder();

		ReceiveMessageRequest request = new ReceiveMessageRequest(AWS_SQS_QUEUE_URL).withWaitTimeSeconds(1)
				.withMaxNumberOfMessages(10);

		List<Message> sqsMessages = sqsClient.receiveMessage(request).getMessages();
		for (Message message : sqsMessages) {
			//run process for message
			System.out.println(message.getBody());
			
			//dequeue message after using it
			//also perfect step so check if message was successfully processed
			dequeuMessageFromSQS(message);
		}
		return sqsMessages;
	}

	public void dequeuMessageFromSQS(Message message) {
		AmazonSQS sqsClient = sqsClientBuilder();

		sqsClient.deleteMessage(new DeleteMessageRequest()
				  .withQueueUrl(AWS_SQS_QUEUE_URL)
				  .withReceiptHandle(message.getReceiptHandle()));

	}
}
