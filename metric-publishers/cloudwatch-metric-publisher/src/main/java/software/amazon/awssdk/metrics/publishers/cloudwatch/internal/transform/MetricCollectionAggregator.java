package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform.DetailedMetricAggregator.DetailedMetrics;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

public class MetricCollectionAggregator {
    public static final int MAX_METRIC_DATA_PER_REQUEST = 20;
    public static final int MAX_VALUES_PER_REQUEST = 300;

    private final String namespace;
    private final Set<SdkMetric<String>> dimensions;
    private final Set<MetricCategory> metricCategories;

    private final TimeBucketedMetrics timeBucketedMetrics;

    public MetricCollectionAggregator(String namespace,
                                      Set<SdkMetric<String>> dimensions,
                                      Set<MetricCategory> metricCategories,
                                      Set<SdkMetric<?>> detailedMetrics) {
        this.namespace = namespace;
        this.dimensions = dimensions;
        this.metricCategories = metricCategories;
        this.timeBucketedMetrics = new TimeBucketedMetrics(dimensions, metricCategories, detailedMetrics);
    }

    public void addCollection(MetricCollection collection) {
        timeBucketedMetrics.addMetrics(collection);
    }

    public List<PutMetricDataRequest> getRequests() {
        List<PutMetricDataRequest> requests = new ArrayList<>();

        List<MetricDatum> requestMetricDatums = new ArrayList<>();
        ValuesInRequestCounter valuesInRequestCounter = new ValuesInRequestCounter();

        Map<Instant, Collection<MetricAggregator>> metrics = timeBucketedMetrics.timeBucketedMetrics();

        for (Map.Entry<Instant, Collection<MetricAggregator>> entry : metrics.entrySet()) {
            Instant timeBucket = entry.getKey();
            for (MetricAggregator metric : entry.getValue()) {
                if (requestMetricDatums.size() >= MAX_METRIC_DATA_PER_REQUEST) {
                    requests.add(newPutRequest(requestMetricDatums));
                    requestMetricDatums.clear();
                }

                metric.ifSummary(summaryAggregator -> requestMetricDatums.add(summaryMetricDatum(timeBucket, summaryAggregator)));

                metric.ifDetailed(detailedAggregator -> {
                    int startIndex = 0;
                    Collection<DetailedMetrics> detailedMetrics = detailedAggregator.detailedMetrics();

                    while (startIndex < detailedMetrics.size()) {
                        if (valuesInRequestCounter.get() >= MAX_VALUES_PER_REQUEST) {
                            requests.add(newPutRequest(requestMetricDatums));
                            requestMetricDatums.clear();
                            valuesInRequestCounter.reset();
                        }

                        MetricDatum data = detailedMetricDatum(timeBucket, detailedAggregator,
                                                               startIndex, MAX_VALUES_PER_REQUEST - valuesInRequestCounter.get());
                        int valuesAdded = data.values().size();
                        startIndex += valuesAdded;
                        valuesInRequestCounter.add(valuesAdded);
                        requestMetricDatums.add(data);
                    }
                });
            }
        }

        if (!requestMetricDatums.isEmpty()) {
            requests.add(newPutRequest(requestMetricDatums));
        }

        timeBucketedMetrics.reset();

        return requests;
    }

    private MetricDatum detailedMetricDatum(Instant timeBucket,
                                            DetailedMetricAggregator metric,
                                            int metricStartIndex,
                                            int maxElements) {
        List<Double> values = new ArrayList<>();
        List<Double> counts = new ArrayList<>();

        metric.detailedMetrics()
              .stream()
              .skip(metricStartIndex)
              .limit(maxElements)
              .forEach(detailedMetrics -> {
            values.add(detailedMetrics.metricValue());
            counts.add((double) detailedMetrics.metricCount());
        });

        return MetricDatum.builder()
                          .timestamp(timeBucket)
                          .metricName(metric.metric().name())
                          .dimensions(metric.dimensions())
                          .unit(metric.unit())
                          .values(values)
                          .counts(counts)
                          .build();
    }

    private MetricDatum summaryMetricDatum(Instant timeBucket,
                                           SummaryMetricAggregator metric) {
        StatisticSet stats = StatisticSet.builder()
                                         .minimum(metric.min())
                                         .maximum(metric.max())
                                         .sum(metric.sum())
                                         .sampleCount((double) metric.count())
                                         .build();
        return MetricDatum.builder()
                          .timestamp(timeBucket)
                          .metricName(metric.metric().name())
                          .dimensions(metric.dimensions())
                          .unit(metric.unit())
                          .statisticValues(stats)
                          .build();
    }

    private PutMetricDataRequest newPutRequest(List<MetricDatum> metricData) {
        return PutMetricDataRequest.builder()
                                   .namespace(namespace)
                                   .metricData(metricData)
                                   .build();
    }

    private static class ValuesInRequestCounter {
        private int valuesInRequest;

        private void add(int i) {
            valuesInRequest += i;
        }

        private int get() {
            return valuesInRequest;
        }

        private void reset() {
            valuesInRequest = 0;
        }
    }
}
