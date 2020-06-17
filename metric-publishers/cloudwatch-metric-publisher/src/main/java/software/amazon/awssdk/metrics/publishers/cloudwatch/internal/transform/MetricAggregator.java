package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform;

import java.util.List;
import java.util.function.Consumer;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

interface MetricAggregator {
    SdkMetric<?> metric();

    List<Dimension> dimensions();

    void addMetricValue(double value);

    StandardUnit unit();

    default void ifSummary(Consumer<SummaryMetricAggregator> aggregator) {
        if (this instanceof SummaryMetricAggregator) {
            aggregator.accept((SummaryMetricAggregator) this);
        }
    }

    default void ifDetailed(Consumer<DetailedMetricAggregator> aggregator) {
        if (this instanceof DetailedMetricAggregator) {
            aggregator.accept((DetailedMetricAggregator) this);
        }
    }
}
