package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform;

import java.util.List;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;

class MetricAggregatorKey {
    private final SdkMetric<?> metric;
    private final List<Dimension> dimensions;

    MetricAggregatorKey(SdkMetric<?> metric, List<Dimension> dimensions) {
        this.metric = metric;
        this.dimensions = dimensions;
    }

    public final SdkMetric<?> metric() {
        return this.metric;
    }

    public final List<Dimension> dimensions() {
        return this.dimensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MetricAggregatorKey that = (MetricAggregatorKey) o;

        if (!metric.equals(that.metric)) {
            return false;
        }
        return dimensions.equals(that.dimensions);
    }

    @Override
    public int hashCode() {
        int result = metric.hashCode();
        result = 31 * result + dimensions.hashCode();
        return result;
    }
}
