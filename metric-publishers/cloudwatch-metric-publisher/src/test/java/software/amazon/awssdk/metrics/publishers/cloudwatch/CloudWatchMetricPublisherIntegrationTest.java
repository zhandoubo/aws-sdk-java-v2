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
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.Http2Metric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollector;

public class CloudWatchMetricPublisherIntegrationTest {
    @Test
    public void test() throws InterruptedException {
        try (CloudWatchMetricPublisher publisher =
                 CloudWatchMetricPublisher.builder()
                                          .publishFrequency(Duration.ofSeconds(60))
                                          .metricQueueSize(5_000)
                                          .build()) {

            for (int i = 0; i < Integer.MAX_VALUE; ++i) {
                // TODO: What about child collections?!
                MetricCollector collector = MetricCollector.create("test");
                collector.reportMetric(CoreMetric.SERVICE_ID, "TestService");
                collector.reportMetric(CoreMetric.OPERATION_NAME, "TestOperation");
                collector.reportMetric(HttpMetric.MAX_CONCURRENCY, ThreadLocalRandom.current().nextInt(0, 10000));
                collector.reportMetric(HttpMetric.LEASED_CONCURRENCY, ThreadLocalRandom.current().nextInt(0, 10000));
                collector.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, ThreadLocalRandom.current().nextInt(0, 10000));
                collector.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, ThreadLocalRandom.current().nextInt(0, 10000));
                collector.reportMetric(Http2Metric.LOCAL_STREAM_WINDOW_SIZE_IN_BYTES, ThreadLocalRandom.current().nextInt(0, 10000));
                collector.reportMetric(Http2Metric.REMOTE_STREAM_WINDOW_SIZE_IN_BYTES, ThreadLocalRandom.current().nextInt(0, 10000));
                publisher.publish(collector.collect());
                Thread.sleep(10);
            }
        }
    }

}