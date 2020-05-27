package software.amazon.awssdk.http;

import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.SdkMetric;

public final class HttpMetrics {
    public static final SdkMetric<String> HTTP_CLIENT_NAME = metric("HttpClientName", String.class);

    public static final SdkMetric<Integer> MAX_POOLED_CONNECTIONS = metric("MaxPooledConnections", Integer.class);

    public static final SdkMetric<Integer> AVAILABLE_POOLED_CONNECTIONS = metric("AvailablePooledConnections", Integer.class);

    public static final SdkMetric<Integer> LEASED_CONNECTIONS = metric("LeasedConnections", Integer.class);

    public static final SdkMetric<Integer> PENDING_REQUESTS = metric("PendingRequests", Integer.class);

    private HttpMetrics() {
    }

    private static <T> SdkMetric<T> metric(String name, Class<T> clzz) {
        return SdkMetric.create(name, clzz, MetricCategory.DEFAULT, MetricCategory.HTTP_CLIENT);
    }
}
