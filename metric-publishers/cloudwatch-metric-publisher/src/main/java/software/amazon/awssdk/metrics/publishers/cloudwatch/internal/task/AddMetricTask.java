package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task;

import software.amazon.awssdk.metrics.MetricCollection;

public class AddMetricTask implements MetricTask {
    private MetricCollection metricCollection;

    public AddMetricTask(MetricCollection metricCollection) {
        this.metricCollection = metricCollection;
    }

    @Override
    public void run() {

    }
}
