package software.amazon.awssdk.http.apache.internal;

import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.SdkMetric;

public class NoOpMetricCollector implements MetricCollector {
    @Override
    public String name() {
        return "NoOp";
    }

    @Override
    public <T> void reportMetric(SdkMetric<T> metric, T data) {
    }

    @Override
    public MetricCollector createChild(String name) {
        throw new UnsupportedOperationException("No op collector does not support createChild");
    }

    @Override
    public MetricCollection collect() {
        throw new UnsupportedOperationException("No op collector does not support collect");
    }
}
