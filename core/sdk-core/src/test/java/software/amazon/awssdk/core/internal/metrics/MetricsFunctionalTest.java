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

package software.amazon.awssdk.core.internal.metrics;


import java.util.function.Consumer;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MetricsFunctionalTest {
    public static class Execution {
        private Consumer<SdkMetricCollection> metricSubscriber;

        public Execution(Consumer<SdkMetricCollection> metricSubscriber) {
            this.metricSubscriber = metricSubscriber;
        }

        public void execute() {
            SdkMetricCollector metricsCollector = SdkMetricCollector.create();

            try (SdkDurationMetricCollector executionDuration =
                     SdkDurationMetricCollector.create(SdkMetrics.EXECUTION_DURATION, metricsCollector)) {
                for (int i = 1; i <= 3; ++i) {
                    attemptExecution(i, metricsCollector);
                }
            }

            metricSubscriber.accept(metricsCollector.collect());
        }

        private void attemptExecution(int attemptNumber, SdkMetricCollector metricsCollector) {
            try (SdkDurationMetricCollector attemptDuration =
                     SdkDurationMetricCollector.create(SdkMetrics.EXECUTION_ATTEMPT_DURATION, metricsCollector)) {
                metricsCollector.reportMetric(SdkMetrics.BYTES_SENT, 1234L);
            }
        }
    }
}