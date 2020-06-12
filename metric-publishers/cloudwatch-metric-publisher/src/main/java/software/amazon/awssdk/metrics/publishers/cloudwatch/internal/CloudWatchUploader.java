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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;

public class CloudWatchUploader {
    private final CloudWatchAsyncClient cloudWatchClient;

    public CloudWatchUploader(CloudWatchAsyncClient cloudWatchClient,
                              int maxMetricQueueSize) {
        this.cloudWatchClient = cloudWatchClient;
    }

    public CompletableFuture<Void> upload(List<PutMetricDataRequest> requests) {
        CompletableFuture<?>[] publishResults = startCalls(requests);
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

    private CompletableFuture<?>[] startCalls(List<PutMetricDataRequest> requests) {
        return requests.stream()
                       .peek(System.out::println)
                       .map(cloudWatchClient::putMetricData)
                       .toArray(CompletableFuture[]::new);
    }

    public void close(boolean closeClient) {
        if (closeClient) {
            this.cloudWatchClient.close();
        }
    }
}
