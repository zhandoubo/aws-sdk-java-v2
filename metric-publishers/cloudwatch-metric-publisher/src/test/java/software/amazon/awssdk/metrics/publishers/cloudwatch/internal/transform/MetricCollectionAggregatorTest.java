package software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

public class MetricCollectionAggregatorTest {
    private static final String DEFAULT_NAMESPACE = "namespace";
    private static final Set<SdkMetric<String>> DEFAULT_DIMENSIONS = Stream.of(CoreMetric.SERVICE_ID, CoreMetric.OPERATION_NAME)
                                                                           .collect(Collectors.toSet());
    private static final Set<MetricCategory> DEFAULT_CATEGORIES = Collections.singleton(MetricCategory.HTTP_CLIENT);
    private static final Set<SdkMetric<?>> DEFAULT_DETAILED_METRICS = Collections.emptySet();

    @Test
    public void maximumRequestsIsHonored() {
        assertThat(aggregatorWithUniqueMetricsAdded(MetricCollectionAggregator.MAX_METRIC_DATA_PER_REQUEST).getRequests())
            .hasSize(1);
        assertThat(aggregatorWithUniqueMetricsAdded(MetricCollectionAggregator.MAX_METRIC_DATA_PER_REQUEST + 1).getRequests())
            .hasSize(2);
    }

    @Test
    public void maximumMetricValuesIsHonored() {
        assertThat(aggregatorWithUniqueValuesAdded(HttpMetric.MAX_CONCURRENCY,
                                                   MetricCollectionAggregator.MAX_VALUES_PER_REQUEST).getRequests())
            .hasSize(1);
        assertThat(aggregatorWithUniqueValuesAdded(HttpMetric.MAX_CONCURRENCY,
                                                   MetricCollectionAggregator.MAX_VALUES_PER_REQUEST + 1).getRequests())
            .hasSize(2);
    }

    @Test
    public void dimensionOrderInCollectionDoesNotMatter() {
        MetricCollectionAggregator aggregator = defaultAggregator();

        MetricCollector collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(CoreMetric.OPERATION_NAME, "OperationName");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 1);
        aggregator.addCollection(collector.collect());

        collector = collector();
        collector.reportMetric(CoreMetric.OPERATION_NAME, "OperationName");
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 2);
        aggregator.addCollection(collector.collect());

        assertThat(aggregator.getRequests()).hasOnlyOneElementSatisfying(request -> {
            assertThat(request.metricData()).hasSize(1);
        });
    }

    @Test
    public void metricsAreAggregatedByDimensionAndMetric() {
        MetricCollectionAggregator aggregator = defaultAggregator();

        MetricCollector collector = collector();
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 1);
        aggregator.addCollection(collector.collect());

        collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 2);
        aggregator.addCollection(collector.collect());

        collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(CoreMetric.OPERATION_NAME, "OperationName");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 3);
        collector.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 4);
        aggregator.addCollection(collector.collect());

        assertThat(aggregator.getRequests()).hasOnlyOneElementSatisfying(request -> {
            assertThat(request.namespace()).isEqualTo(DEFAULT_NAMESPACE);
            assertThat(request.metricData()).hasSize(4).allSatisfy(data -> {
                assertThat(data.values()).isEmpty();
                assertThat(data.counts()).isEmpty();
                if (data.dimensions().isEmpty()) {
                    assertThat(data.metricName()).isEqualTo(HttpMetric.MAX_CONCURRENCY.name());
                    assertThat(data.statisticValues().sampleCount()).isEqualTo(1);
                    assertThat(data.statisticValues().sum()).isEqualTo(1);
                } else if (data.dimensions().size() == 1) {
                    assertThat(data.metricName()).isEqualTo(HttpMetric.MAX_CONCURRENCY.name());
                    assertThat(data.statisticValues().sampleCount()).isEqualTo(1);
                    assertThat(data.statisticValues().sum()).isEqualTo(2);
                } else {
                    assertThat(data.dimensions().size()).isEqualTo(2);
                    if (data.metricName().equals(HttpMetric.MAX_CONCURRENCY.name())) {
                        assertThat(data.statisticValues().sampleCount()).isEqualTo(1);
                        assertThat(data.statisticValues().sum()).isEqualTo(3);
                    } else {
                        assertThat(data.metricName()).isEqualTo(HttpMetric.AVAILABLE_CONCURRENCY.name());
                        assertThat(data.statisticValues().sampleCount()).isEqualTo(1);
                        assertThat(data.statisticValues().sum()).isEqualTo(4);
                    }
                }
            });
        });
    }

    @Test
    public void metricSummariesAreCorrectWithValuesInSameCollector() {
        MetricCollectionAggregator aggregator = defaultAggregator();
        MetricCollector collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 2);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 1);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 4);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 4);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 3);
        aggregator.addCollection(collector.collect());

        assertThat(aggregator.getRequests()).hasOnlyOneElementSatisfying(request -> {
            assertThat(request.namespace()).isEqualTo(DEFAULT_NAMESPACE);
            assertThat(request.metricData()).hasOnlyOneElementSatisfying(metricData -> {
                assertThat(metricData.dimensions()).hasOnlyOneElementSatisfying(dimension -> {
                    assertThat(dimension.name()).isEqualTo(CoreMetric.SERVICE_ID.name());
                    assertThat(dimension.value()).isEqualTo("ServiceId");
                });
                assertThat(metricData.values()).isEmpty();
                assertThat(metricData.counts()).isEmpty();
                assertThat(metricData.statisticValues()).isEqualTo(StatisticSet.builder()
                                                                               .minimum(1.0)
                                                                               .maximum(4.0)
                                                                               .sum(14.0)
                                                                               .sampleCount(5.0)
                                                                               .build());
            });
        });
    }

    @Test
    public void metricSummariesAreCorrectWithValuesInDifferentCollector() {
        MetricCollectionAggregator aggregator = defaultAggregator();

        MetricCollector collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 2);
        aggregator.addCollection(collector.collect());

        collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 1);
        aggregator.addCollection(collector.collect());

        collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 4);
        aggregator.addCollection(collector.collect());

        collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 4);
        aggregator.addCollection(collector.collect());

        collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 3);
        aggregator.addCollection(collector.collect());

        assertThat(aggregator.getRequests()).hasOnlyOneElementSatisfying(request -> {
            assertThat(request.namespace()).isEqualTo(DEFAULT_NAMESPACE);
            assertThat(request.metricData()).hasOnlyOneElementSatisfying(metricData -> {
                assertThat(metricData.dimensions()).hasOnlyOneElementSatisfying(dimension -> {
                    assertThat(dimension.name()).isEqualTo(CoreMetric.SERVICE_ID.name());
                    assertThat(dimension.value()).isEqualTo("ServiceId");
                });
                assertThat(metricData.values()).isEmpty();
                assertThat(metricData.counts()).isEmpty();
                assertThat(metricData.statisticValues()).isEqualTo(StatisticSet.builder()
                                                                               .minimum(1.0)
                                                                               .maximum(4.0)
                                                                               .sum(14.0)
                                                                               .sampleCount(5.0)
                                                                               .build());
            });
        });
    }

    @Test
    public void detailedMetricsAreCorrect() {
        MetricCollectionAggregator aggregator = aggregatorWithCustomDetailedMetrics(HttpMetric.MAX_CONCURRENCY);
        MetricCollector collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 2);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 1);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 4);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 4);
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 3);
        aggregator.addCollection(collector.collect());

        assertThat(aggregator.getRequests()).hasOnlyOneElementSatisfying(request -> {
            assertThat(request.namespace()).isEqualTo(DEFAULT_NAMESPACE);
            assertThat(request.metricData()).hasOnlyOneElementSatisfying(metricData -> {
                assertThat(metricData.dimensions()).hasOnlyOneElementSatisfying(dimension -> {
                    assertThat(dimension.name()).isEqualTo(CoreMetric.SERVICE_ID.name());
                    assertThat(dimension.value()).isEqualTo("ServiceId");
                });

                assertThat(metricData.values()).hasSize(4);
                assertThat(metricData.statisticValues()).isNull();
                for (int i = 0; i < metricData.values().size(); i++) {
                    Double value = metricData.values().get(i);
                    Double count = metricData.counts().get(i);
                    switch (value.toString()) {
                        case "1.0":
                        case "2.0":
                        case "3.0":
                            assertThat(count).isEqualTo(1.0);
                            break;
                        case "4.0":
                            assertThat(count).isEqualTo(2.0);
                            break;
                        default:
                            Assert.fail();
                    }
                }
            });
        });
    }

    @Test
    public void metricsFromOtherCategoriesAreIgnored() {
        MetricCollectionAggregator aggregator = defaultAggregator();
        MetricCollector collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(CoreMetric.HTTP_STATUS_CODE, 404);
        aggregator.addCollection(collector.collect());

        assertThat(aggregator.getRequests()).isEmpty();
    }

    @Test
    public void getRequestsResetsState() {
        MetricCollectionAggregator aggregator = defaultAggregator();
        MetricCollector collector = collector();
        collector.reportMetric(CoreMetric.SERVICE_ID, "ServiceId");
        collector.reportMetric(HttpMetric.MAX_CONCURRENCY, 1);
        aggregator.addCollection(collector.collect());

        assertThat(aggregator.getRequests()).hasSize(1);
        assertThat(aggregator.getRequests()).isEmpty();
    }

    @Test
    public void numberTypesAreTransformedCorrectly() {
        SdkMetric<CustomNumber> metric = someMetric(CustomNumber.class);
        assertThat(transformMetricValueUsingAggregator(metric, new CustomNumber(-1000.5))).isEqualTo(-1000.5);
        assertThat(transformMetricValueUsingAggregator(metric, new CustomNumber(0))).isEqualTo(0);
        assertThat(transformMetricValueUsingAggregator(metric, new CustomNumber(1000.5))).isEqualTo(1000.5);
    }

    @Test
    public void durationsAreTransformedCorrectly() {
        SdkMetric<Duration> metric = someMetric(Duration.class);
        assertThat(transformMetricValueUsingAggregator(metric, Duration.ofSeconds(-10))).isEqualTo(-10_000);
        assertThat(transformMetricValueUsingAggregator(metric, Duration.ofSeconds(0))).isEqualTo(0);
        assertThat(transformMetricValueUsingAggregator(metric, Duration.ofSeconds(10))).isEqualTo(10_000);
    }

    private <T> Double transformMetricValueUsingAggregator(SdkMetric<T> metric, T input) {
        MetricCollectionAggregator aggregator = aggregatorWithCustomDetailedMetrics(metric);
        MetricCollector collector = collector();
        collector.reportMetric(metric, input);
        aggregator.addCollection(collector.collect());

        return aggregator.getRequests().get(0).metricData().get(0).values().get(0);
    }

    private MetricCollectionAggregator aggregatorWithUniqueValuesAdded(SdkMetric<Integer> metric, int numValues) {
        MetricCollectionAggregator aggregator = aggregatorWithCustomDetailedMetrics(metric);
        for (int i = 0; i < numValues; i++) {
            MetricCollector collector = collector();
            collector.reportMetric(metric, i);
            aggregator.addCollection(collector.collect());
        }
        return aggregator;
    }

    private MetricCollectionAggregator aggregatorWithUniqueMetricsAdded(int numMetrics) {
        MetricCollectionAggregator aggregator = defaultAggregator();
        MetricCollector collector = collector();
        for (int i = 0; i < numMetrics; i++) {
            collector.reportMetric(someMetric(), 0);
        }
        aggregator.addCollection(collector.collect());
        return aggregator;
    }

    private MetricCollectionAggregator defaultAggregator() {
        return new MetricCollectionAggregator(DEFAULT_NAMESPACE,
                                              DEFAULT_DIMENSIONS,
                                              DEFAULT_CATEGORIES,
                                              DEFAULT_DETAILED_METRICS);
    }

    private MetricCollectionAggregator aggregatorWithCustomDetailedMetrics(SdkMetric<?>... detailedMetrics) {
        return new MetricCollectionAggregator(DEFAULT_NAMESPACE,
                                              DEFAULT_DIMENSIONS,
                                              DEFAULT_CATEGORIES,
                                              Stream.of(detailedMetrics).collect(Collectors.toSet()));
    }

    private MetricCollector collector() {
        return MetricCollector.create("test");
    }

    private SdkMetric<Integer> someMetric() {
        return someMetric(Integer.class);
    }

    private <T> SdkMetric<T> someMetric(Class<T> clazz) {
        return SdkMetric.create(getClass().getSimpleName() + UUID.randomUUID().toString(),
                                clazz, MetricCategory.HTTP_CLIENT);
    }

    private static class CustomNumber extends Number {
        private final double value;

        public CustomNumber(double value) {
            this.value = value;
        }

        @Override
        public int intValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long longValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float floatValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double doubleValue() {
            return value;
        }
    }
}