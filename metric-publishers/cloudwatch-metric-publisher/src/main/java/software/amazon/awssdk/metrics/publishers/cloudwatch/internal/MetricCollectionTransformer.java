package software.amazon.awssdk.metrics.publishers.cloudwatch.internal;

import static java.time.temporal.ChronoUnit.MINUTES;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;
import software.amazon.awssdk.utils.Validate;

public class MetricCollectionTransformer {
    private static final int MAX_METRIC_DATA_PER_REQUEST = 20;
    private static final int MAX_VALUES_PER_REQUEST = 300;

    private final String namespace;
    private final Set<SdkMetric<String>> dimensions;
    private final Set<MetricCategory> metricCategories;
    private final boolean metricCategoriesContainsAll;
    private final Set<SdkMetric<?>> nonSummaryMetrics;

    public MetricCollectionTransformer(String namespace,
                                       Set<SdkMetric<String>> dimensions,
                                       Set<MetricCategory> metricCategories,
                                       Set<SdkMetric<?>> nonSummaryMetrics) {
        this.namespace = namespace;
        this.dimensions = dimensions;
        this.metricCategories = metricCategories;
        this.metricCategoriesContainsAll = metricCategories.contains(MetricCategory.ALL);
        this.nonSummaryMetrics = nonSummaryMetrics;
    }

    public List<PutMetricDataRequest> transform(List<MetricCollection> collections) {
        TimeBucketedMetrics timeBucketedMetrics = new TimeBucketedMetrics();
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

                while (startIndex < metric.valuesAndCounts.size()) {
                    if (metricData.size() >= MAX_METRIC_DATA_PER_REQUEST || valuesInRequest >= MAX_VALUES_PER_REQUEST) {
                        requests.add(newPutRequest(metricData));
                        metricData.clear();
                        valuesInRequest = 0;
                    }

                    if (isSummaryMetric(metric)) {
                        metricData.add(summaryMetricDatum(timeBucket, metric));
                        ++valuesInRequest;
                        startIndex += metric.valuesAndCounts.size();
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
        return !this.nonSummaryMetrics.contains(metric.aggregatorKey.metric);
    }

    private MetricDatum detailedMetricDatum(Instant timeBucket,
                                            SdkMetricAggregator metric,
                                            int metricStartIndex,
                                            int maxElements) {
        List<Double> values = new ArrayList<>();
        List<Double> counts = new ArrayList<>();

        metric.valuesAndCounts.entrySet()
                              .stream()
                              .skip(metricStartIndex)
                              .limit(maxElements).forEach(valueAndCount -> {
            values.add(valueAndCount.getKey());
            counts.add((double) valueAndCount.getValue());
        });

        return MetricDatum.builder()
                          .timestamp(timeBucket)
                          .metricName(metric.aggregatorKey.metric.name())
                          .dimensions(metric.aggregatorKey.dimensions)
                          .unit(metric.unit)
                          .values(values)
                          .counts(counts)
                          .build();
    }

    private MetricDatum summaryMetricDatum(Instant timeBucket,
                                           SdkMetricAggregator metric) {
        Validate.validState(!metric.valuesAndCounts.isEmpty(), "Values must not be empty.");

        int count = 0;
        double sum = 0;
        double min = Integer.MAX_VALUE;
        double max = Integer.MIN_VALUE;

        for (Map.Entry<Double, Integer> valueAndCount : metric.valuesAndCounts.entrySet()) {
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
                          .metricName(metric.aggregatorKey.metric.name())
                          .dimensions(metric.aggregatorKey.dimensions)
                          .unit(metric.unit)
                          .statisticValues(stats)
                          .build();
    }

    private PutMetricDataRequest newPutRequest(List<MetricDatum> metricData) {
        return PutMetricDataRequest.builder()
                                   .namespace(namespace)
                                   .metricData(metricData)
                                   .build();
    }

    private class TimeBucketedMetrics {
        private final Map<Instant, Map<SdkMetricAggregatorKey, SdkMetricAggregator>> timeBucketedMetrics = new HashMap<>();

        public Map<Instant, Collection<SdkMetricAggregator>> getTimeBucketedMetrics() {
            return timeBucketedMetrics.entrySet()
                                      .stream()
                                      .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().values()));
        }

        public void addMetrics(MetricCollection metrics) {
            Instant bucket = getBucket(metrics);
            addMetricsToBucket(metrics, bucket);
        }

        private Instant getBucket(MetricCollection metrics) {
            return metrics.creationTime().truncatedTo(MINUTES);
        }

        private void addMetricsToBucket(MetricCollection metrics, Instant bucketId) {
            aggregateMetrics(metrics, timeBucketedMetrics.computeIfAbsent(bucketId, i -> new HashMap<>()));
        }

        private void aggregateMetrics(MetricCollection metrics, Map<SdkMetricAggregatorKey, SdkMetricAggregator> bucket) {
            List<Dimension> dimensions = dimensions(metrics);
            extractAllMetrics(metrics).forEach(metricRecord -> {
                SdkMetricAggregatorKey aggregatorKey = new SdkMetricAggregatorKey(metricRecord.metric(), dimensions);
                valueFor(metricRecord).ifPresent(metricValue -> {
                    bucket.computeIfAbsent(aggregatorKey, m -> newAggregator(aggregatorKey))
                          .addValue(metricValue);
                });
            });
        }

        private List<Dimension> dimensions(MetricCollection metricCollection) {
            List<Dimension> result = new ArrayList<>();
            for (MetricRecord<?> metricRecord : metricCollection) {
                if (dimensions.contains(metricRecord.metric())) {
                    result.add(Dimension.builder()
                                        .name(metricRecord.metric().name())
                                        .value((String) metricRecord.value())
                                        .build());
                }
            }
            result.sort(Comparator.comparing(Dimension::name));
            return result;
        }

        private List<MetricRecord<?>> extractAllMetrics(MetricCollection metrics) {
            List<MetricRecord<?>> result = new ArrayList<>();
            extractAllMetrics(metrics, result);
            return result;
        }

        private void extractAllMetrics(MetricCollection metrics, List<MetricRecord<?>> extractedMetrics) {
            for (MetricRecord<?> metric : metrics) {
                extractedMetrics.add(metric);
            }
            metrics.children().forEach(child -> extractAllMetrics(child, extractedMetrics));
        }

        private SdkMetricAggregator newAggregator(SdkMetricAggregatorKey aggregatorKey) {
            return new SdkMetricAggregator(aggregatorKey, unitFor(aggregatorKey.metric));
        }

        private StandardUnit unitFor(SdkMetric<?> metric) {
            Class<?> metricType = metric.valueClass();

            if (Duration.class.isAssignableFrom(metricType)) {
                return StandardUnit.MILLISECONDS;
            }
            return StandardUnit.NONE;
        }

        private Optional<Double> valueFor(MetricRecord<?> metricRecord) {
            if (!hasReportedCategory(metricRecord)) {
                return Optional.empty();
            }

            Class<?> metricType = metricRecord.metric().valueClass();

            if (Duration.class.isAssignableFrom(metricType)) {
                Duration durationMetricValue = (Duration) metricRecord.value();
                long millis = durationMetricValue.toMillis();
                return Optional.of((double) millis);
            } else if (Number.class.isAssignableFrom(metricType)) {
                Number numberMetricValue = (Number) metricRecord.value();
                return Optional.of(numberMetricValue.doubleValue());
            }

            return Optional.empty();
        }

        private boolean hasReportedCategory(MetricRecord<?> metricRecord) {
            return metricCategoriesContainsAll ||
                   metricRecord.metric()
                               .categories()
                               .stream()
                               .anyMatch(metricCategories::contains);
        }
    }

    private class SdkMetricAggregatorKey {
        private final SdkMetric<?> metric;
        private final List<Dimension> dimensions;

        private SdkMetricAggregatorKey(SdkMetric<?> metric, List<Dimension> dimensions) {
            this.metric = metric;
            this.dimensions = dimensions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SdkMetricAggregatorKey that = (SdkMetricAggregatorKey) o;

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

    private class SdkMetricAggregator {
        private final SdkMetricAggregatorKey aggregatorKey;
        private final StandardUnit unit;
        private final Map<Double, Integer> valuesAndCounts = new HashMap<>();

        private SdkMetricAggregator(SdkMetricAggregatorKey aggregatorKey, StandardUnit unit) {
            this.aggregatorKey = aggregatorKey;
            this.unit = unit;
        }

        public Map<Double, Integer> getValuesAndCounts() {
            return Collections.unmodifiableMap(valuesAndCounts);
        }

        public void addValue(Double value) {
            valuesAndCounts.compute(value, (v, c) -> c == null ? 1 : c + 1);
        }

        public StandardUnit getUnit() {
            return unit;
        }
    }
}
