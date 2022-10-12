package org.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.socket.PortFactory;


import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class DeleteObjectTest {

    private ClientAndServer mockServer;
    private AmazonS3 s3;
    private final String bucket = "test-bucket";
    private final String fileKey = "file-key";

    @Before
    public void startServer() {
        mockServer = startClientAndServer(PortFactory.findFreePort());

        AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials("test-access-key", "test-secret-key"));

        s3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(provider)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                "http://localhost:" + mockServer.getPort(),
                                "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .build();

    }

    @After
    public void stopServer() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }


    @Test
    public void testDeleteObject() {
        mockServer.when(
                        request()
                                .withMethod("DELETE")
                                .withPath("/" + bucket + "/" + fileKey)
                                .withHeader("x-amz-request-payer", "requester"))
                .respond(
                        response()
                                .withStatusCode(204));
        final DeleteObjectRequest request = new DeleteObjectRequest(bucket, fileKey)
                .withRequesterPays(true);
        s3.deleteObject(request);
    }

    @Test
    public void testDeleteObjects() {
        mockServer.when(
                        request()
                                .withMethod("POST")
                                .withPath("/" + bucket + "/")
                                .withQueryStringParameter("delete")
                                .withHeader("x-amz-request-payer", "requester"))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                                        "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
                                        "<Deleted>" +
                                        "<Key>" + fileKey + "</Key>" +
                                        "</Deleted>" +
                                        "</DeleteResult>"));

        final DeleteObjectsRequest request = new DeleteObjectsRequest(bucket)
                .withKeys(fileKey)
                .withRequesterPays(true);
        s3.deleteObjects(request);
    }
}
