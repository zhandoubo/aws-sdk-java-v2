package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task;

import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform.MetricCollectionAggregator;

public class AddMetricTask implements MetricTask {
    private final MetricCollectionAggregator collectionAggregator;
    private final MetricCollection metricCollection;

    public AddMetricTask(MetricCollectionAggregator collectionAggregator,
                         MetricCollection metricCollection) {
        this.collectionAggregator = collectionAggregator;
        this.metricCollection = metricCollection;
    }

    @Override
    public void run() {
        collectionAggregator.addCollection(metricCollection);
    }
}
