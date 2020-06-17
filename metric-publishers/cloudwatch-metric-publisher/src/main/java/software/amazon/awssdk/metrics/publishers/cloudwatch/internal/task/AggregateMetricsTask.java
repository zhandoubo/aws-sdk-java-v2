package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform.MetricCollectionAggregator;

/**
 * A task that is executed on the {@link CloudWatchMetricPublisher}'s executor to add a {@link MetricCollection} to a
 * {@link MetricCollectionAggregator}.
 */
@SdkInternalApi
public class AggregateMetricsTask implements Runnable {
    private final MetricCollectionAggregator collectionAggregator;
    private final MetricCollection metricCollection;

    public AggregateMetricsTask(MetricCollectionAggregator collectionAggregator,
                                MetricCollection metricCollection) {
        this.collectionAggregator = collectionAggregator;
        this.metricCollection = metricCollection;
    }

    @Override
    public void run() {
        collectionAggregator.addCollection(metricCollection);
    }
}
