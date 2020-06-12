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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

public class MetricConsumer {
    private final MetricCollectionTransformer transformer;

    private final CloudWatchAsyncClient cloudWatchClient;
    private final Queue<MetricCollection> metricQueue;
    private final Duration publishFrequency;
    private final ScheduledExecutorService aggregatorThread;

    public MetricConsumer(CloudWatchAsyncClient cloudWatchClient,
                          int maxMetricQueueSize,
                          Set<SdkMetric<String>> dimensions,
                          Set<MetricCategory> metricCategories,
                          Set<SdkMetric<?>> nonSummaryMetrics,
                          String namespace,
                          Duration publishFrequency) {
        this.cloudWatchClient = cloudWatchClient;
        this.metricQueue = new LinkedBlockingQueue<>(maxMetricQueueSize);
        this.publishFrequency = publishFrequency;
        this.transformer = new MetricCollectionTransformer(namespace, dimensions, metricCategories, nonSummaryMetrics);
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
        List<CompletableFuture<?>> publishResults = startCalls();
        List<Throwable> failures = waitForCallsToComplete(publishResults);

        if (failures.isEmpty()) {
            System.out.println("Published " + publishResults.size() + " requests.");
        } else {
            System.out.println(failures.size() + " out of " + publishResults.size() + " requests failed to "
                               + "publish. One failure reason follows.");
            failures.get(0).printStackTrace();
        }
    }

    private List<CompletableFuture<?>> startCalls() {
        List<MetricCollection> collections = new ArrayList<>();
        MetricCollection collection = metricQueue.poll();
        while (collection != null) {
            collections.add(collection);
            collection = metricQueue.poll();
        }

        return transformer.transform(collections)
                          .stream()
                          .peek(System.out::println)
                          .map(cloudWatchClient::putMetricData)
                          .collect(Collectors.toList());
    }

    private List<Throwable> waitForCallsToComplete(List<CompletableFuture<?>> publishResults) {
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
        return failures;
    }

    public void close(boolean closeClient) {
        if (closeClient) {
            this.cloudWatchClient.close();
        }
        this.aggregatorThread.shutdown();
    }
}
