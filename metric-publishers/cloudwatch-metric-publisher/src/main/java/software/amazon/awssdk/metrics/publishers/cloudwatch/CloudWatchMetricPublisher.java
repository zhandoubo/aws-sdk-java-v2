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

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.metrics.publishers.cloudwatch.internal.MetricConsumer;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

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

    private final boolean closeClientWithPublisher;
    private final MetricConsumer metricConsumer;

    private CloudWatchMetricPublisher(Builder builder) {
        this.closeClientWithPublisher = resolveCloseClientWithPublisher(builder);
        this.metricConsumer = new MetricConsumer(resolveClient(builder),
                                                 resolveMetricQueueSize(builder),
                                                 resolveDimensions(builder),
                                                 resolveNamespace(builder),
                                                 resolvePublishFrequency(builder));
    }

    private Set<SdkMetric<String>> resolveDimensions(Builder builder) {
        return builder.dimensions == null ? DEFAULT_DIMENSIONS : builder.dimensions;
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
        metricConsumer.publish(metricCollection);
    }

    @Override
    public void close() {
        metricConsumer.close(closeClientWithPublisher);
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
        public Set<SdkMetric<String>> dimensions;
        private CloudWatchAsyncClient client;
        private Duration publishFrequency;
        private String namespace;
        private Integer metricQueueSize;

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

        /**
         * @param publishFrequency the timeout between consecutive {@link CloudWatchMetricPublisher#publish()} calls
         * @return This object for method chaining
         */
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

        /**
         * @return an instance of {@link CloudWatchMetricPublisher}
         */
        public CloudWatchMetricPublisher build() {
            return new CloudWatchMetricPublisher(this);
        }
    }
}
