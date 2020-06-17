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

import static software.amazon.awssdk.metrics.publishers.cloudwatch.internal.CloudWatchMetricLogger.METRIC_LOGGER;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
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
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.MetricUploader;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task.AddMetricTask;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.task.FlushMetricsTask;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.transform.MetricCollectionAggregator;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

/**
 * An implementation of {@link MetricPublisher} that aggregates and uploads metrics to Amazon CloudWatch on a periodic basis.
 *
 * <p><b>Usage</b>
 *
 * <p>TODO
 *
 * <p>All metrics from the publisher are logged in the {@code software.amazon.awssdk.metrics.publishers.cloudwatch} namespace.
 *
 * <p><b>Configuration</b>
 *
 * <p>The default settings of the metrics publisher are meant to minimize memory usage and CloudWatch cost, while still
 * providing a useful amount of insight into the performance of AWS and the SDK. Care should be taken when overriding the default
 * values on the publisher, because they can result in an associated increased in memory usage and CloudWatch cost.
 *
 * <p>See {@link Builder} for the configuration values that are available for the publisher, and how they can be used to increase
 * the functionality or decrease the cost the publisher.
 *
 * <p><b>Warning:</b> Make sure the {@link #close()} this publisher when it is done being used to release all resources it
 * consumes. Failure to do so will result in possible thread or file descriptor leaks.
 */
@SdkPublicApi
public final class CloudWatchMetricPublisher implements MetricPublisher {
    private static final int MAXIMUM_TASK_QUEUE_SIZE = 1000;

    private static final String DEFAULT_NAMESPACE = "AwsSdk/JavaSdk2";
    private static final int DEFAULT_MAXIMUM_CALLS_PER_UPLOAD = 10;
    private static final Duration DEFAULT_UPLOAD_FREQUENCY = Duration.ofMinutes(1);
    private static final Set<SdkMetric<String>> DEFAULT_DIMENSIONS = Stream.of(CoreMetric.SERVICE_ID, CoreMetric.OPERATION_NAME)
                                                                           .collect(Collectors.toSet());
    private static final Set<MetricCategory> DEFAULT_METRIC_CATEGORIES = Stream.of(MetricCategory.DEFAULT, MetricCategory.HTTP_CLIENT)
                                                                               .collect(Collectors.toSet());
    private static final Set<SdkMetric<?>> DEFAULT_DETAILED_METRICS = Collections.emptySet();

    private final boolean closeClientWithPublisher;
    private final MetricUploader metricUploader;
    private final MetricCollectionAggregator metricAggregator;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService executor;
    private final int maximumCallsPerUpload;

    private CloudWatchMetricPublisher(Builder builder) {
        this.closeClientWithPublisher = resolveCloseClientWithPublisher(builder);
        this.metricAggregator = new MetricCollectionAggregator(resolveNamespace(builder),
                                                               resolveDimensions(builder),
                                                               resolveMetricCategories(builder),
                                                               resolveDetailedMetrics(builder));
        this.metricUploader = new MetricUploader(resolveClient(builder));
        this.maximumCallsPerUpload = resolveMaximumCallsPerUpload(builder);

        ThreadFactory threadFactory = new ThreadFactoryBuilder().threadNamePrefix("cloud-watch-metric-publisher").build();

        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                               new LinkedBlockingQueue<>(MAXIMUM_TASK_QUEUE_SIZE),
                                               threadFactory);
        long flushFrequencyInMillis = resolveUploadFrequency(builder).toMillis();
        this.scheduledExecutor.scheduleAtFixedRate(this::flushMetrics,
                                                   flushFrequencyInMillis, flushFrequencyInMillis, TimeUnit.MILLISECONDS);
    }

    private Set<MetricCategory> resolveMetricCategories(Builder builder) {
        return builder.metricCategories == null ? DEFAULT_METRIC_CATEGORIES : new HashSet<>(builder.metricCategories);
    }

    private Set<SdkMetric<?>> resolveDetailedMetrics(Builder builder) {
        return builder.detailedMetrics == null ? DEFAULT_DETAILED_METRICS : new HashSet<>(builder.detailedMetrics);
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

    private Duration resolveUploadFrequency(Builder builder) {
        return builder.uploadFrequency == null ? DEFAULT_UPLOAD_FREQUENCY : builder.uploadFrequency;
    }

    private String resolveNamespace(Builder builder) {
        return builder.namespace == null ? DEFAULT_NAMESPACE : builder.namespace;
    }

    private int resolveMaximumCallsPerUpload(Builder builder) {
        return builder.maximumCallsPerUpload == null ? DEFAULT_MAXIMUM_CALLS_PER_UPLOAD : builder.maximumCallsPerUpload;
    }

    @Override
    public void publish(MetricCollection metricCollection) {
        try {
            executor.submit(new AddMetricTask(metricAggregator, metricCollection));
        } catch (RejectedExecutionException e) {
            METRIC_LOGGER.warn(() -> "Some AWS SDK client-side metrics have been dropped because an internal executor did not "
                                     + "accept them. This usually occurs because your publisher has been shut down or you have "
                                     + "generated too many requests for the publisher to handle in a timely fashion.", e);
        }
    }

    private void flushMetrics() {
        while (true) {
            try {
                executor.execute(new FlushMetricsTask(metricAggregator, metricUploader, maximumCallsPerUpload));
                break;
            } catch (RejectedExecutionException e) {
                Thread.yield();
            }
        }
    }

    @Override
    public void close() {
        scheduledExecutor.shutdownNow();

        flushMetrics();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        metricUploader.close(closeClientWithPublisher);
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
     * Builder class to construct {@link CloudWatchMetricPublisher} instances. See the individual properties for which
     * configuration settings are available.
     */
    public static final class Builder {
        private CloudWatchAsyncClient client;
        private Duration uploadFrequency;
        private String namespace;
        private Integer maximumCallsPerUpload;
        private Collection<SdkMetric<String>> dimensions;
        private Collection<MetricCategory> metricCategories;
        private Collection<SdkMetric<?>> detailedMetrics;

        private Builder() {
        }

        /**
         * Configure the {@link CloudWatchAsyncClient} instance that should be used to communicate with CloudWatch.
         *
         * <p>If this is not specified, the {@code CloudWatchAsyncClient} will be created via
         * {@link CloudWatchAsyncClient#create()} (and will be closed when {@link #close()} is invoked).
         *
         * <p>If you specify a {@code CloudWatchAsyncClient} via this method, it <i>will not</i> be closed when this publisher
         * is closed. You will need to need to manage the lifecycle of the client yourself.
         */
        public Builder cloudWatchClient(CloudWatchAsyncClient client) {
            this.client = client;
            return this;
        }

        /**
         * Configure the frequency at which aggregated metrics are uploaded to CloudWatch and released from memory.
         *
         * <p>If this is not specified, metrics will be uploaded once per minute.
         *
         * <p>Smaller values will: (1) reduce the amount of memory used by the library (particularly when
         * {@link #detailedMetrics(Collection)} are enabled), (2) increase the number of CloudWatch calls (and therefore
         * increase CloudWatch usage cost).
         *
         * <p>Larger values will: (1) increase the amount of memory used by the library (particularly when
         * {@code detailedMetrics} are enabled), (2) increase the time it takes for metric data to appear in
         * CloudWatch, (3) reduce the number of CloudWatch calls (and therefore decrease CloudWatch usage cost).
         *
         * <p><b>Warning:</b> When {@code detailedMetrics} are enabled, all unique metric values are stored in memory until they
         * can be published to CloudWatch. A high {@code uploadFrequency} with multiple {@code detailedMetrics} enabled can
         * quickly consume heap memory while the values wait to be published to CloudWatch. In memory constrained environments, it
         * is recommended to minimize the number of {@code detailedMetrics} configured on the publisher, or to upload metric data
         * more frequently. As with all performance and resource concerns, profiling in a production-like environment is
         * encouraged.
         */
        public Builder uploadFrequency(Duration uploadFrequency) {
            this.uploadFrequency = uploadFrequency;
            return this;
        }

        /**
         * Configure the maximum number of {@link CloudWatchAsyncClient#putMetricData(PutMetricDataRequest)} calls that an
         * individual "upload" event can make to CloudWatch. Any metrics that would exceed this limit are dropped during the
         * upload, logging a warning on the {@code software.amazon.awssdk.metrics.publishers.cloudwatch} namespace.
         *
         * <p>The SDK will always attempt to maximize the number of metrics per put-metric-data call, but uploads will be split
         * into multiple put-metric-data calls if they include a lot of different metrics or if there are a lot of high-value-
         * distribution {@link #detailedMetrics(Collection)} being monitored.
         *
         * <p>This value combined with the {@link #uploadFrequency(Duration)} effectively provide a "hard cap" on the number of
         * put-metric-data calls, to prevent unbounded cost in the event that too many metrics are enabled by the user.
         *
         * <p>If this is not specified, put-metric-data calls will be capped at 10 per upload.
         */
        public Builder maximumCallsPerUpload(Integer maximumCallsPerUpload) {
            this.maximumCallsPerUpload = maximumCallsPerUpload;
            return this;
        }

        /**
         * Configure the {@link PutMetricDataRequest#namespace()} used for all put-metric-data calls from this publisher.
         *
         * <p>If this is not specified, {@code AwsSdk/JavaSdk2} will be used.
         */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * Configure the {@link SdkMetric}s that are used to define the {@link Dimension}s metrics are aggregated under.
         *
         * <p>If this is not specified, {@link CoreMetric#SERVICE_ID} and {@link CoreMetric#OPERATION_NAME} are used, allowing
         * you to compare metrics for different services and operations.
         *
         * <p><b>Warning:</b> Configuring the dimensions incorrectly can result in a large increase in the number of unique
         * metrics and put-metric-data calls to cloudwatch, which have an associated monetary cost. Be sure you're choosing your
         * metric dimensions wisely, and that you always evaluate the cost of modifying these values on your monthly usage costs.
         *
         * <p><b>Example useful settings:</b>
         * <ul>
         * <li>{@code CoreMetric.SERVICE_ID} and {@code CoreMetric.OPERATION_NAME} (default): Separate metrics by service and
         * operation, so that you can compare latencies between AWS services and operations.</li>
         * <li>{@code CoreMetric.SERVICE_ID}, {@code CoreMetric.OPERATION_NAME} and {@code CoreMetric.HOST_NAME}: Separate
         * metrics by service, operation and host so that you can compare latencies across hosts in your fleet. Note: This should
         * only be used when your fleet is relatively small. Large fleets result in a large number of unique metrics being
         * generated.</li>
         * <li>{@code CoreMetric.SERVICE_ID}, {@code CoreMetric.OPERATION_NAME} and {@code HttpMetric.HTTP_CLIENT_NAME}: Separate
         * metrics by service, operation and HTTP client type so that you can compare latencies between different HTTP client
         * implementations.</li>
         * </ul>
         */
        public Builder dimensions(Collection<SdkMetric<String>> dimensions) {
            this.dimensions = new ArrayList<>(dimensions);
            return this;
        }

        /**
         * @
         * @param dimensions
         * @return
         */
        @SafeVarargs
        public final Builder dimensions(SdkMetric<String>... dimensions) {
            return dimensions(Arrays.asList(dimensions));
        }

        public Builder metricCategories(Collection<MetricCategory> metricCategories) {
            this.metricCategories = new ArrayList<>(metricCategories);
            return this;
        }

        public Builder metricCategories(MetricCategory... metricCategories) {
            return metricCategories(Arrays.asList(metricCategories));
        }

        public Builder detailedMetrics(Collection<SdkMetric<?>> detailedMetrics) {
            this.detailedMetrics = new ArrayList<>(detailedMetrics);
            return this;
        }

        public Builder detailedMetrics(SdkMetric<?>... detailedMetrics) {
            return detailedMetrics(Arrays.asList(detailedMetrics));
        }

        /**
         * @return an instance of {@link CloudWatchMetricPublisher}
         */
        public CloudWatchMetricPublisher build() {
            return new CloudWatchMetricPublisher(this);
        }
    }
}
