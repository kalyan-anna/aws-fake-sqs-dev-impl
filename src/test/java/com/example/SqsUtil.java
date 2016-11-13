package com.example;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQSClient;

import static com.amazonaws.regions.Regions.US_WEST_2;

public class SqsUtil {

	public static AmazonSQSClient createSQS() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (~/.aws/credentials), and is in valid format.",
					e);
		}
		AmazonSQSClient sqs = new AmazonSQSClient(credentials);
		sqs.setRegion(Region.getRegion(US_WEST_2));
		return sqs;
	}

}
