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

package software.amazon.awssdk.metrics.publishers.cloudwatch.internal;

import static software.amazon.awssdk.metrics.publishers.cloudwatch.internal.CloudWatchMetricLogger.METRIC_LOGGER;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

public class MetricConsumer {
    private final CloudWatchAsyncClient cloudWatchClient;
    private final Queue<MetricCollection> metricQueue;
    private final Set<SdkMetric<String>> dimensions;
    private final String namespace;
    private Duration publishFrequency;
    private final ScheduledExecutorService aggregatorThread;

    public MetricConsumer(CloudWatchAsyncClient cloudWatchClient,
                          int maxMetricQueueSize,
                          Set<SdkMetric<String>> dimensions,
                          String namespace,
                          Duration publishFrequency) {
        this.cloudWatchClient = cloudWatchClient;
        this.metricQueue = new LinkedBlockingQueue<>(maxMetricQueueSize);
        this.dimensions = dimensions;
        this.namespace = namespace;
        this.publishFrequency = publishFrequency;
        this.aggregatorThread = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                                             .threadNamePrefix("cloudwatch-metrics")
                                                                             .build());
        aggregatorThread.scheduleAtFixedRate(this::flushMetricQueue,
                                             publishFrequency.toMillis(),
                                             publishFrequency.toMillis(),
                                             TimeUnit.MILLISECONDS);
    }

    public void publish(MetricCollection metricCollection) {
        if (!metricQueue.offer(metricCollection)) {
            System.out.println("Request metrics have been dropped because the cloudwatch metric queue is full.");
        }
    }

    private void flushMetricQueue() {
        List<CompletableFuture<?>> publishResults = new ArrayList<>();
        MetricCollection metricCollection = metricQueue.poll();
        while (metricCollection != null) {
            toPutMetricDataRequest(metricCollection)
                .map(cloudWatchClient::putMetricData)
                .ifPresent(publishResults::add);

            metricCollection = metricQueue.poll();
        }

        List<Throwable> failures = new ArrayList<>();
        for (CompletableFuture<?> publishResult : publishResults) {
            try {
                publishResult.get(5, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                failures.add(e);
            } catch (ExecutionException e) {
                failures.add(e.getCause());
            }
        }

        if (failures.isEmpty()) {
            System.out.println("Published " + publishResults.size() + " metric collections.");
        } else {
            System.out.println(failures.size() + " out of " + publishResults.size() + " metric collections failed to "
                               + "publish. One failure reason follows.");
            failures.get(0).printStackTrace();
        }
    }

    private Optional<PutMetricDataRequest> toPutMetricDataRequest(MetricCollection metricCollection) {
        List<MetricDatum> metricData = toMetricData(metricCollection);
        if (metricData.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(PutMetricDataRequest.builder()
                                               .namespace(namespace)
                                               .metricData(metricData)
                                               .build());
    }

    private List<MetricDatum> toMetricData(MetricCollection metricCollection) {
        List<Dimension> dimensions = dimensions(metricCollection);
        List<MetricDatum> result = new ArrayList<>();
        for (MetricRecord<?> metricRecord : metricCollection) {
            toMetricData(metricCollection, dimensions, metricRecord).ifPresent(result::add);
        }
        return result;
    }

    private Optional<MetricDatum> toMetricData(MetricCollection metricCollection,
                                               List<Dimension> dimensions,
                                               MetricRecord<?> metricRecord) {

        return extractMetricData(metricRecord).map(d -> MetricDatum.builder()
                                                                   .dimensions(dimensions)
                                                                   .metricName(metricRecord.metric().name())
                                                                   .timestamp(metricCollection.creationTime())
                                                                   .value(d.value)
                                                                   .unit(d.unit)
                                                                   .build());
    }

    private Optional<SdkMetricData> extractMetricData(MetricRecord<?> metricRecord) {
        Class<?> metricType = metricRecord.metric().valueClass();
        if (Number.class.isAssignableFrom(metricType)) {
            return Optional.of(extractMetricData((Number) metricRecord.value()));
        } else if (Duration.class.isAssignableFrom(metricType)) {
            return Optional.of(extractMetricData((Duration) metricRecord.value()));
        }

        return Optional.empty();
    }

    private SdkMetricData extractMetricData(Number value) {
        return new SdkMetricData(value.doubleValue(), StandardUnit.NONE);
    }

    private SdkMetricData extractMetricData(Duration value) {
        return new SdkMetricData(value.toMillis(), StandardUnit.MILLISECONDS);
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
        return result;
    }

    public void close(boolean closeClient) {
        if (closeClient) {
            this.cloudWatchClient.close();
        }
        this.aggregatorThread.shutdown();
    }

    private class SdkMetricData {
        private final double value;
        private final StandardUnit unit;

        private SdkMetricData(double value, StandardUnit unit) {
            this.value = value;
            this.unit = unit;
        }
    }
}
