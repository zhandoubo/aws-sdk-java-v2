package software.amazon.awssdk.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.endpointdiscoveryrequiredtest.EndpointDiscoveryRequiredTestAsyncClient;
import software.amazon.awssdk.services.endpointdiscoveryrequiredtest.EndpointDiscoveryRequiredTestClient;
import software.amazon.awssdk.services.endpointdiscoveryrequiredwithcustomizationtest.EndpointDiscoveryRequiredWithCustomizationTestAsyncClient;
import software.amazon.awssdk.services.endpointdiscoveryrequiredwithcustomizationtest.EndpointDiscoveryRequiredWithCustomizationTestClient;
import software.amazon.awssdk.services.endpointdiscoverytest.EndpointDiscoveryTestAsyncClient;
import software.amazon.awssdk.services.endpointdiscoverytest.EndpointDiscoveryTestClient;

/**
 * Allow a customer setting an endpoint override on their client to disable endpoint discovery, even when
 * endpointDiscoveryEnabled = true.
 *
 * When false:
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = True, EndpointOverride = Set } => <b>FAILURE</b>
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = True, EndpointOverride = null } => ENDPOINT_FROM_DISCOVERY
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = False, EndpointOverride = Set } => FAILURE
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = False, EndpointOverride = null } => FAILURE
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = True, EndpointOverride = Set } => FAILURE
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = True, EndpointOverride = null } => ENDPOINT_FROM_DISCOVERY
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = False, EndpointOverride = Set } => ENDPOINT_FROM_OVERRIDE
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = False, EndpointOverride = null } => ENDPOINT_FROM_REGION
 *
 * When true:
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = True, EndpointOverride = Set } => <b>ENDPOINT_FROM_OVERRIDE</b>
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = True, EndpointOverride = null } => ENDPOINT_FROM_DISCOVERY
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = False, EndpointOverride = Set } => FAILURE
 * { EndpointDiscoveryRequired = True, EndpointDiscoveryEnabled = False, EndpointOverride = null } => FAILURE
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = True, EndpointOverride = Set } => UNDEFINED
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = True, EndpointOverride = null } => UNDEFINED
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = False, EndpointOverride = Set } => UNDEFINED
 * { EndpointDiscoveryRequired = False, EndpointDiscoveryEnabled = False, EndpointOverride = null } => UNDEFINED
 *
 * Note: EndpointDiscoveryEnabled = true IFF all operations in the service have EndpointDiscoveryRequired = true
 */
@RunWith(Parameterized.class)
public class RequiredEndpointDiscoveryTest {
    private static final String OPTIONAL_SERVICE_ENDPOINT = "https://awsendpointdiscoverytestservice.us-west-2.amazonaws.com";
    private static final String REQUIRED_SERVICE_ENDPOINT = "https://awsendpointdiscoveryrequiredtestservice.us-west-2.amazonaws.com";
    private static final String REQUIRED_CUSTOMIZED_SERVICE_ENDPOINT = "https://awsendpointdiscoveryrequiredwithcustomizationtestservice.us-west-2.amazonaws.com";
    private static final String ENDPOINT_OVERRIDE = "https://endpointoverride";

    private static final List<TestCase<?>> ALL_TEST_CASES = new ArrayList<>();

    private final TestCase<?> testCase;

    static {
        // This first case (case 0) is different than other SDKs/the SEP. This should probably actually throw an exception.
        ALL_TEST_CASES.addAll(endpointDiscoveryOptionalCases(true, true, ENDPOINT_OVERRIDE + "/DescribeEndpoints", ENDPOINT_OVERRIDE + "/TestDiscoveryOptional"));
        ALL_TEST_CASES.addAll(endpointDiscoveryOptionalCases(true, false, OPTIONAL_SERVICE_ENDPOINT + "/DescribeEndpoints", OPTIONAL_SERVICE_ENDPOINT + "/TestDiscoveryOptional"));
        ALL_TEST_CASES.addAll(endpointDiscoveryOptionalCases(false, true, ENDPOINT_OVERRIDE + "/TestDiscoveryOptional"));
        ALL_TEST_CASES.addAll(endpointDiscoveryOptionalCases(false, false, OPTIONAL_SERVICE_ENDPOINT + "/TestDiscoveryOptional"));

        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredCases(true, true));
        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredCases(true, false, REQUIRED_SERVICE_ENDPOINT + "/DescribeEndpoints"));
        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredCases(false, true));
        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredCases(false, false));

        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredAndCustomizedCases(true, true, ENDPOINT_OVERRIDE, "/TestDiscoveryRequired"));
        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredAndCustomizedCases(true, false, REQUIRED_CUSTOMIZED_SERVICE_ENDPOINT + "/DescribeEndpoints"));
        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredAndCustomizedCases(false, true));
        ALL_TEST_CASES.addAll(endpointDiscoveryRequiredAndCustomizedCases(false, false));
    }

    public RequiredEndpointDiscoveryTest(TestCase<?> testCase) {
        this.testCase = testCase;
    }

    @Before
    public void reset() {
        EndpointCapturingInterceptor.reset();
    }

    @Parameterized.Parameters(name = "{index} - {0}")
    public static List<TestCase<?>> testCases() {
        return ALL_TEST_CASES;
    }

    @Test(timeout = 5_000)
    public void invokeTestCase() {
        try {
            testCase.callClient();
        } catch (Throwable e) {
            if (e instanceof CompletionException) {
                // Unwrap async exceptions so that they can be tested the same as async ones.
                e = e.getCause();
            }
            assertThat(e).isInstanceOf(SdkClientException.class); // TODO: Be more specific
        }

        if (testCase.enforcePathOrder) {
            assertThat(EndpointCapturingInterceptor.endpoints).containsExactly(testCase.expectedPaths);
        } else {
            // Async is involved when order doesn't matter, so wait a little while until the expected number of paths arrive.
            while (EndpointCapturingInterceptor.endpoints.size() < testCase.expectedPaths.length) {
                Thread.yield();
            }
            assertThat(EndpointCapturingInterceptor.endpoints).containsExactlyInAnyOrder(testCase.expectedPaths);
        }
    }

    private static List<TestCase<?>> endpointDiscoveryOptionalCases(boolean endpointDiscoveryEnabled,
                                                                    boolean endpointOverridden,
                                                                    String... expectedEndpoints) {
        TestCase<?> syncCase = new TestCase<>(createClient(EndpointDiscoveryTestClient.builder().endpointDiscoveryEnabled(endpointDiscoveryEnabled),
                                                           endpointOverridden),
                                              c -> c.testDiscoveryOptional(r -> {}),
                                              caseName(EndpointDiscoveryTestClient.class, endpointDiscoveryEnabled, endpointOverridden, expectedEndpoints),
                                              false,
                                              expectedEndpoints);

        TestCase<?> asyncCase = new TestCase<>(createClient(EndpointDiscoveryTestAsyncClient.builder().endpointDiscoveryEnabled(endpointDiscoveryEnabled),
                                                            endpointOverridden),
                                               c -> c.testDiscoveryOptional(r -> {}).join(),
                                               caseName(EndpointDiscoveryTestAsyncClient.class, endpointDiscoveryEnabled, endpointOverridden, expectedEndpoints),
                                               false,
                                               expectedEndpoints);

        return Arrays.asList(syncCase, asyncCase);
    }

    private static List<TestCase<?>> endpointDiscoveryRequiredCases(boolean endpointDiscoveryEnabled,
                                                                    boolean endpointOverridden,
                                                                    String... expectedEndpoints) {
        TestCase<?> syncCase = new TestCase<>(createClient(EndpointDiscoveryRequiredTestClient.builder().endpointDiscoveryEnabled(endpointDiscoveryEnabled),
                                                           endpointOverridden),
                                              c -> c.testDiscoveryRequired(r -> {}),
                                              caseName(EndpointDiscoveryRequiredTestClient.class, endpointDiscoveryEnabled, endpointOverridden, expectedEndpoints),
                                              true,
                                              expectedEndpoints);

        TestCase<?> asyncCase = new TestCase<>(createClient(EndpointDiscoveryRequiredTestAsyncClient.builder().endpointDiscoveryEnabled(endpointDiscoveryEnabled),
                                                            endpointOverridden),
                                               c -> c.testDiscoveryRequired(r -> {}).join(),
                                               caseName(EndpointDiscoveryRequiredTestAsyncClient.class, endpointDiscoveryEnabled, endpointOverridden, expectedEndpoints),
                                               true,
                                               expectedEndpoints);

        return Arrays.asList(syncCase, asyncCase);
    }

    private static List<TestCase<?>> endpointDiscoveryRequiredAndCustomizedCases(boolean endpointDiscoveryEnabled,
                                                                                 boolean endpointOverridden,
                                                                                 String... expectedEndpoints) {
        TestCase<?> syncCase = new TestCase<>(createClient(EndpointDiscoveryRequiredWithCustomizationTestClient.builder().endpointDiscoveryEnabled(endpointDiscoveryEnabled),
                                                           endpointOverridden),
                                              c -> c.testDiscoveryRequired(r -> {}),
                                              caseName(EndpointDiscoveryRequiredWithCustomizationTestClient.class, endpointDiscoveryEnabled, endpointOverridden, expectedEndpoints),
                                              true,
                                              expectedEndpoints);

        TestCase<?> asyncCase = new TestCase<>(createClient(EndpointDiscoveryRequiredWithCustomizationTestAsyncClient.builder().endpointDiscoveryEnabled(endpointDiscoveryEnabled),
                                                            endpointOverridden),
                                               c -> c.testDiscoveryRequired(r -> {}).join(),
                                               caseName(EndpointDiscoveryRequiredWithCustomizationTestAsyncClient.class, endpointDiscoveryEnabled, endpointOverridden, expectedEndpoints),
                                               true,
                                               expectedEndpoints);

        return Arrays.asList(syncCase, asyncCase);
    }

    private static <T> T createClient(SdkClientBuilder<?, T> clientBuilder,
                                      boolean endpointOverridden) {
        return clientBuilder.applyMutation(c -> addEndpointOverride(c, endpointOverridden))
                            .overrideConfiguration(c -> c.retryPolicy(p -> p.numRetries(0))
                                                         .addExecutionInterceptor(new EndpointCapturingInterceptor()))
                            .build();
    }

    private static String caseName(Class<?> client,
                                   boolean endpointDiscoveryEnabled,
                                   boolean endpointOverridden,
                                   String... expectedEndpoints) {
        return "(Client=" + client.getSimpleName() +
               ", DiscoveryEnabled=" + endpointDiscoveryEnabled +
               ", EndpointOverridden=" + endpointOverridden +
               ") => (ExpectedEndpoints=" + Arrays.toString(expectedEndpoints) + ")";
    }

    private static void addEndpointOverride(SdkClientBuilder<?, ?> builder, boolean endpointOverridden) {
        if (endpointOverridden) {
            builder.endpointOverride(URI.create(ENDPOINT_OVERRIDE));
        }
    }

    private static class TestCase<T> {
        private final T client;
        private final Consumer<T> methodCall;
        private final String caseName;
        private final boolean enforcePathOrder;
        private final String[] expectedPaths;

        private TestCase(T client, Consumer<T> methodCall, String caseName, boolean enforcePathOrder, String... expectedPaths) {
            this.client = client;
            this.methodCall = methodCall;
            this.caseName = caseName;
            this.enforcePathOrder = enforcePathOrder;
            this.expectedPaths = expectedPaths;
        }

        private void callClient() {
            methodCall.accept(client);
        }

        @Override
        public String toString() {
            return caseName;
        }
    }

    private static class EndpointCapturingInterceptor implements ExecutionInterceptor {
        private static List<String> endpoints = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            endpoints.add(context.httpRequest().getUri().toString());
        }

        private static void reset() {
            endpoints.clear();
        }
    }
}
