package software.amazon.awssdk.cloudsearchdomain;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudsearchdomain.CloudSearchDomainAsyncClient;
import software.amazon.awssdk.services.cloudsearchdomain.CloudSearchDomainClient;
import software.amazon.awssdk.services.cloudsearchdomain.model.ContentType;
import software.amazon.awssdk.services.cloudsearchdomain.model.UploadDocumentsRequest;
import software.amazon.awssdk.testutils.service.AwsIntegrationTestBase;

public class SignedAsyncUploadIntegrationTest extends AwsIntegrationTestBase {
    private static final String DOMAIN = "doc-sdk-domain-1552806277831-bnm46tlqxklmuulvst5ofb3wde.us-west-2.cloudsearch.amazonaws.com";
    private final String DOCUMENT = "" +
            "[\n" +
            "{\n" +
            "    \"type\": \"add\",\n" +
            "    \"id\": \"foo\",\n" +
            "    \"fields\": {\n" +
            "        \"sdkindex1552806265478\": \"some value\"\n" +
            "    }\n" +
            "}\n" +
            "]";

    private static CloudSearchDomainAsyncClient client;
    private static CloudSearchDomainClient syncClient;

    @BeforeClass
    public static void setup() {
        URI endpoint = URI.create("https://" + DOMAIN);

        client = CloudSearchDomainAsyncClient.builder()
                .endpointOverride(endpoint)
                .region(Region.of("us-west-2"))
                .credentialsProvider(ProfileCredentialsProvider.create("java-integ-test"))
                .build();

        syncClient = CloudSearchDomainClient.builder()
                .endpointOverride(endpoint)
                .region(Region.of("us-west-2"))
                .credentialsProvider(ProfileCredentialsProvider.create("java-integ-test"))
                .build();
    }

    @AfterClass
    public static void teardown() {
        client.close();
    }

    @Test
    @Ignore
    public void testUploadSync() {
        byte[] documentBytes = DOCUMENT.getBytes(StandardCharsets.UTF_8);

        UploadDocumentsRequest req = UploadDocumentsRequest.builder()
                .contentType(ContentType.APPLICATION_JSON)
                .contentLength((long) documentBytes.length)
                .build();

        syncClient.uploadDocuments(req, RequestBody.fromBytes(documentBytes));
    }

    @Test
    public void testUpload() {
        byte[] documentBytes = DOCUMENT.getBytes(StandardCharsets.UTF_8);

        UploadDocumentsRequest req = UploadDocumentsRequest.builder()
                .contentType(ContentType.APPLICATION_JSON)
                .contentLength((long) documentBytes.length)
                .build();

        AsyncRequestBody requestBody = AsyncRequestBody.fromBytes(documentBytes);

        client.uploadDocuments(req, requestBody).join();
    }
}
