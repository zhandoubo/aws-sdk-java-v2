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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

public class CloudWatchUploader {
    private final CloudWatchAsyncClient cloudWatchClient;
    private final Queue<PutMetricDataRequest> publishQueue;

    public CloudWatchUploader(CloudWatchAsyncClient cloudWatchClient,
                              int maxMetricQueueSize) {
        this.cloudWatchClient = cloudWatchClient;
        this.publishQueue = new LinkedBlockingQueue<>(maxMetricQueueSize);
    }

    public void addToUploadQueue(PutMetricDataRequest request) {
        if (!publishQueue.offer(request)) {
            System.out.println("Request metrics have been dropped because the cloudwatch metric queue is full.");
        }
    }

    public CompletableFuture<Void> flushUploadQueue() {
        CompletableFuture<?>[] publishResults = startCalls();
        return CompletableFuture.allOf(publishResults).whenComplete((r, t) -> {
            int numRequests = publishResults.length;
            if (t != null) {
                System.out.println("Failed while publishing a portion of " + numRequests + " requests.");
                t.printStackTrace();
            } else {
                System.out.println("Published " + numRequests + " requests.");
            }
        });
    }

    private CompletableFuture<?>[] startCalls() {
        List<PutMetricDataRequest> requests = new ArrayList<>();
        PutMetricDataRequest request = publishQueue.poll();
        while (request != null) {
            requests.add(request);
            request = publishQueue.poll();
        }

        return requests.stream()
                       .peek(System.out::println)
                       .map(cloudWatchClient::putMetricData)
                       .toArray(CompletableFuture[]::new);
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
    }
}
