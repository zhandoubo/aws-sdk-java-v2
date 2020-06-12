package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform;

import static java.time.temporal.ChronoUnit.MINUTES;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

class TimeBucketedMetrics {
    private final Map<Instant, Map<SdkMetricAggregatorKey, SdkMetricAggregator>> timeBucketedMetrics = new HashMap<>();
    private final Set<SdkMetric<String>> dimensions;
    private final Set<MetricCategory> metricCategories;
    private final boolean metricCategoriesContainsAll;

    public TimeBucketedMetrics(Set<SdkMetric<String>> dimensions,
                               Set<MetricCategory> metricCategories) {
        this.dimensions = dimensions;
        this.metricCategories = metricCategories;
        this.metricCategoriesContainsAll = metricCategories.contains(MetricCategory.ALL);
    }

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
        return new SdkMetricAggregator(aggregatorKey, unitFor(aggregatorKey.metric()));
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
