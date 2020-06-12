/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.metrics.publishers.cloudwatch;

import static java.util.Collections.singletonList;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.CloudWatchUploader;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task.AddMetricTask;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task.FlushMetricsTask;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform.MetricCollectionTransformer;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

/**
 * An implementation of {@link MetricPublisher} that uploads metrics to Amazon CloudWatch.
 */
@SdkPublicApi
public final class CloudWatchMetricPublisher implements MetricPublisher {
    private static final String DEFAULT_NAMESPACE = "AwsSdk/JavaSdk2x";
    private static final int DEFAULT_QUEUE_SIZE = 1000;
    private static final Duration DEFAULT_PUBLISH_FREQUENCY = Duration.ofMinutes(1);
    private static final Set<SdkMetric<String>> DEFAULT_DIMENSIONS = Stream.of(CoreMetric.SERVICE_ID, CoreMetric.OPERATION_NAME)
                                                                           .collect(Collectors.toSet());
    private static final Set<MetricCategory> DEFAULT_METRIC_CATEGORIES = Stream.of(MetricCategory.DEFAULT, MetricCategory.HTTP_CLIENT)
                                                                               .collect(Collectors.toSet());
    private static final Set<SdkMetric<?>> DEFAULT_NON_SUMMARY_METRICS = Collections.emptySet();

    private final boolean closeClientWithPublisher;
    private final CloudWatchUploader cloudWatchUploader;
    private final MetricCollectionTransformer metricTransformer;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService executor;

    private CloudWatchMetricPublisher(Builder builder) {
        this.closeClientWithPublisher = resolveCloseClientWithPublisher(builder);
        this.metricTransformer = new MetricCollectionTransformer(resolveNamespace(builder),
                                                                 resolveDimensions(builder),
                                                                 resolveMetricCategories(builder),
                                                                 resolveNonSummaryMetrics(builder));
        this.cloudWatchUploader = new CloudWatchUploader(resolveClient(builder),
                                                         resolveMetricQueueSize(builder));

        ThreadFactory threadFactory = new ThreadFactoryBuilder().threadNamePrefix("cloud-watch-metric-publisher").build();

        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                               new LinkedBlockingQueue<>(resolveMetricQueueSize(builder)),
                                               threadFactory);
        long flushFrequencyInMillis = resolvePublishFrequency(builder).toMillis();
        this.scheduledExecutor.scheduleAtFixedRate(() -> executor.execute(new FlushMetricsTask()),
                                                   flushFrequencyInMillis, flushFrequencyInMillis, TimeUnit.MILLISECONDS);
    }

    private Set<MetricCategory> resolveMetricCategories(Builder builder) {
        return builder.metricCategories == null ? DEFAULT_METRIC_CATEGORIES : new HashSet<>(builder.metricCategories);
    }

    private Set<SdkMetric<?>> resolveNonSummaryMetrics(Builder builder) {
        return builder.nonSummaryMetrics == null ? DEFAULT_NON_SUMMARY_METRICS : new HashSet<>(builder.nonSummaryMetrics);
    }

    private Set<SdkMetric<String>> resolveDimensions(Builder builder) {
        return builder.dimensions == null ? DEFAULT_DIMENSIONS : new HashSet<>(builder.dimensions);
    }

    private boolean resolveCloseClientWithPublisher(Builder builder) {
        return builder.client == null;
    }

    private CloudWatchAsyncClient resolveClient(Builder builder) {
        return builder.client == null ? CloudWatchAsyncClient.create() : builder.client;
    }

    private Duration resolvePublishFrequency(Builder builder) {
        return builder.publishFrequency == null ? DEFAULT_PUBLISH_FREQUENCY : builder.publishFrequency;
    }

    private String resolveNamespace(Builder builder) {
        return builder.namespace == null ? DEFAULT_NAMESPACE : builder.namespace;
    }

    private int resolveMetricQueueSize(Builder builder) {
        return builder.metricQueueSize == null ? DEFAULT_QUEUE_SIZE : builder.metricQueueSize;
    }

    @Override
    public void publish(MetricCollection metricCollection) {
        executor.submit(new AddMetricTask(metricCollection));
    }

    @Override
    public void close() {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        cloudWatchUploader.close(closeClientWithPublisher);
    }

    /**
     * @return A {@link Builder} object to build {@link CloudWatchMetricPublisher}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return A new instance of {@link CloudWatchMetricPublisher} with all defaults.
     */
    public static CloudWatchMetricPublisher create() {
        return builder().build();
    }

    /**
     * Builder class to construct {@link CloudWatchMetricPublisher} instances.
     */
    public static final class Builder {
        private CloudWatchAsyncClient client;
        private Duration publishFrequency;
        private String namespace;
        private Integer metricQueueSize;
        private Collection<SdkMetric<String>> dimensions;
        private Collection<MetricCategory> metricCategories;
        private Collection<SdkMetric<?>> nonSummaryMetrics;

        private Builder() {
        }

        /**
         * @param client async client to use for uploads metrics to Amazon CloudWatch
         * @return This object for method chaining
         */
        public Builder cloudWatchClient(CloudWatchAsyncClient client) {
            this.client = client;
            return this;
        }

        public Builder publishFrequency(Duration publishFrequency) {
            this.publishFrequency = publishFrequency;
            return this;
        }

        /**
         * @param metricQueueSize max number of metrics to store in queue. If the queue is full, new metrics are dropped
         * @return This object for method chaining
         */
        public Builder metricQueueSize(Integer metricQueueSize) {
            this.metricQueueSize = metricQueueSize;
            return this;
        }

        /**
         * @param namespace The CloudWatch namespace for the metric data
         * @return This object for method chaining
         */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder dimensions(Collection<SdkMetric<String>> dimensions) {
            this.dimensions = new ArrayList<>(dimensions);
            return this;
        }

        @SafeVarargs
        public final Builder dimensions(SdkMetric<String>... dimensions) {
            this.dimensions = Arrays.asList(dimensions);
            return this;
        }

        public Builder metricCategories(Collection<MetricCategory> metricCategories) {
            this.metricCategories = new ArrayList<>(metricCategories);
            return this;
        }

        public Builder metricCategories(MetricCategory... metricCategories) {
            return metricCategories(Arrays.asList(metricCategories));
        }

        public Builder nonSummaryMetrics(Collection<SdkMetric<?>> nonSummaryMetrics) {
            this.nonSummaryMetrics = new ArrayList<>(nonSummaryMetrics);
            return this;
        }

        public Builder nonSummaryMetrics(SdkMetric<?>... nonSummaryMetrics) {
            return nonSummaryMetrics(Arrays.asList(nonSummaryMetrics));
        }

        /**
         * @return an instance of {@link CloudWatchMetricPublisher}
         */
        public CloudWatchMetricPublisher build() {
            return new CloudWatchMetricPublisher(this);
        }
    }
}
