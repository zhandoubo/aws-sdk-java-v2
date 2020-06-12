package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

class SdkMetricAggregator {
    private final SdkMetricAggregatorKey aggregatorKey;
    private final StandardUnit unit;
    private final Map<Double, Integer> valuesAndCounts = new HashMap<>();

    SdkMetricAggregator(SdkMetricAggregatorKey aggregatorKey, StandardUnit unit) {
        this.aggregatorKey = aggregatorKey;
        this.unit = unit;
    }

    public SdkMetricAggregatorKey aggregatorKey() {
        return aggregatorKey;
    }

    public StandardUnit unit() {
        return unit;
    }

    public Map<Double, Integer> valuesAndCounts() {
        return Collections.unmodifiableMap(valuesAndCounts);
    }

    public void addValue(Double value) {
        valuesAndCounts.compute(value, (v, c) -> c == null ? 1 : c + 1);
    }

    public StandardUnit getUnit() {
        return unit;
    }
}
