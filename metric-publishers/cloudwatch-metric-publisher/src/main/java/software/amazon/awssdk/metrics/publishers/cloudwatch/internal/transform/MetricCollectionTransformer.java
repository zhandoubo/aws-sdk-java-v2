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
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;
import software.amazon.awssdk.utils.Validate;

public class MetricCollectionTransformer {
    private static final int MAX_METRIC_DATA_PER_REQUEST = 20;
    private static final int MAX_VALUES_PER_REQUEST = 300;

    private final String namespace;
    private final Set<SdkMetric<String>> dimensions;
    private final Set<MetricCategory> metricCategories;
    private final Set<SdkMetric<?>> nonSummaryMetrics;

    public MetricCollectionTransformer(String namespace,
                                       Set<SdkMetric<String>> dimensions,
                                       Set<MetricCategory> metricCategories,
                                       Set<SdkMetric<?>> nonSummaryMetrics) {
        this.namespace = namespace;
        this.dimensions = dimensions;
        this.metricCategories = metricCategories;
        this.nonSummaryMetrics = nonSummaryMetrics;
    }

    public List<PutMetricDataRequest> transform(List<MetricCollection> collections) {
        TimeBucketedMetrics timeBucketedMetrics = new TimeBucketedMetrics(dimensions, metricCategories);
        collections.forEach(timeBucketedMetrics::addMetrics);
        return extractRequests(timeBucketedMetrics);
    }

    private List<PutMetricDataRequest> extractRequests(TimeBucketedMetrics timeBucketedMetrics) {
        List<PutMetricDataRequest> requests = new ArrayList<>();

        List<MetricDatum> metricData = new ArrayList<>();
        int valuesInRequest = 0;

        Map<Instant, Collection<SdkMetricAggregator>> metrics = timeBucketedMetrics.getTimeBucketedMetrics();

        for (Map.Entry<Instant, Collection<SdkMetricAggregator>> entry : metrics.entrySet()) {
            Instant timeBucket = entry.getKey();
            for (SdkMetricAggregator metric : entry.getValue()) {
                int startIndex = 0;

                while (startIndex < metric.valuesAndCounts().size()) {
                    if (metricData.size() >= MAX_METRIC_DATA_PER_REQUEST || valuesInRequest >= MAX_VALUES_PER_REQUEST) {
                        requests.add(newPutRequest(metricData));
                        metricData.clear();
                        valuesInRequest = 0;
                    }

                    if (isSummaryMetric(metric)) {
                        metricData.add(summaryMetricDatum(timeBucket, metric));
                        ++valuesInRequest;
                        startIndex += metric.valuesAndCounts().size();
                    } else {
                        MetricDatum data = detailedMetricDatum(timeBucket, metric,
                                                               startIndex, MAX_VALUES_PER_REQUEST - valuesInRequest);
                        int valuesAdded = data.values().size();
                        startIndex += valuesAdded;
                        valuesInRequest += valuesAdded;
                        metricData.add(data);
                    }
                }
            }
        }

        if (!metricData.isEmpty()) {
            requests.add(newPutRequest(metricData));
        }

        return requests;
    }

    private boolean isSummaryMetric(SdkMetricAggregator metric) {
        return !this.nonSummaryMetrics.contains(metric.aggregatorKey().metric());
    }

    private MetricDatum detailedMetricDatum(Instant timeBucket,
                                            SdkMetricAggregator metric,
                                            int metricStartIndex,
                                            int maxElements) {
        List<Double> values = new ArrayList<>();
        List<Double> counts = new ArrayList<>();

        metric.valuesAndCounts().entrySet()
                                .stream()
                                .skip(metricStartIndex)
                                .limit(maxElements).forEach(valueAndCount -> {
            values.add(valueAndCount.getKey());
            counts.add((double) valueAndCount.getValue());
        });

        return MetricDatum.builder()
                          .timestamp(timeBucket)
                          .metricName(metric.aggregatorKey().metric().name())
                          .dimensions(metric.aggregatorKey().dimensions())
                          .unit(metric.unit())
                          .values(values)
                          .counts(counts)
                          .build();
    }

    private MetricDatum summaryMetricDatum(Instant timeBucket,
                                           SdkMetricAggregator metric) {
        Validate.validState(!metric.valuesAndCounts().isEmpty(), "Values must not be empty.");

        int count = 0;
        double sum = 0;
        double min = Integer.MAX_VALUE;
        double max = Integer.MIN_VALUE;

        for (Map.Entry<Double, Integer> valueAndCount : metric.valuesAndCounts().entrySet()) {
            count += valueAndCount.getValue();
            sum += valueAndCount.getKey() * valueAndCount.getValue();
            min = Double.min(min, valueAndCount.getKey());
            max = Double.max(max, valueAndCount.getKey());
        }

        StatisticSet stats = StatisticSet.builder()
                                         .minimum(min)
                                         .maximum(max)
                                         .sum(sum)
                                         .sampleCount((double) count)
                                         .build();
        return MetricDatum.builder()
                          .timestamp(timeBucket)
                          .metricName(metric.aggregatorKey().metric().name())
                          .dimensions(metric.aggregatorKey().dimensions())
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
}
