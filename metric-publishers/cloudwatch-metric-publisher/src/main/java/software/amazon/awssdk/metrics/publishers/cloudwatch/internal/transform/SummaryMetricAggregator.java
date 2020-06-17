package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform;

import java.util.List;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * An implementation of {@link MetricAggregator} that stores summary statistics for a given metric/dimension pair until the
 * summary can be added to a {@link MetricDatum}.
 */
@SdkInternalApi
class SummaryMetricAggregator implements MetricAggregator {
    private final SdkMetric<?> metric;
    private final List<Dimension> dimensions;
    private final StandardUnit unit;

    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0;
    private int count = 0;

    public SummaryMetricAggregator(MetricAggregatorKey key, StandardUnit unit) {
        this.metric = key.metric();
        this.dimensions = key.dimensions();
        this.unit = unit;
    }

    @Override
    public SdkMetric<?> metric() {
        return metric;
    }

    @Override
    public List<Dimension> dimensions() {
        return dimensions;
    }

    @Override
    public void addMetricValue(double value) {
        min = Double.min(value, min);
        max = Double.max(value, max);
        sum += value;
        ++count;
    }

    @Override
    public StandardUnit unit() {
        return unit;
    }

    public double min() {
        return min;
    }

    public double max() {
        return max;
    }

    public double sum() {
        return sum;
    }

    public int count() {
        return count;
    }
}
