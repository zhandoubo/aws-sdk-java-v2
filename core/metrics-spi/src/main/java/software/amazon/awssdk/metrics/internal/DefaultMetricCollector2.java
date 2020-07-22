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

package software.amazon.awssdk.metrics.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.ToString;
import software.amazon.awssdk.utils.Validate;

/**
 * TODO: Before launch, we should iterate on the performance of this collector, because it's currently very naive.
 */
@SdkInternalApi
public final class DefaultMetricCollector2 implements MetricCollector {
    private static final Logger log = Logger.loggerFor(DefaultMetricCollector2.class);
    private final String name;
    private final Map<SdkMetric<?>, List<MetricRecord<?>>> metrics;
    private final Collection<MetricCollector> children;

    private final Supplier<Map<SdkMetric<?>, List<MetricRecord<?>>>> map;
    private final Supplier<Collection<MetricCollector>> list;

    public DefaultMetricCollector2(String name,
                                   Supplier<Map<SdkMetric<?>, List<MetricRecord<?>>>> map,
                                   Supplier<Collection<MetricCollector>> list) {
        this.name = name;
        this.metrics = map.get();
        this.children = list.get();

        this.map = map;
        this.list = list;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public <T> void reportMetric(SdkMetric<T> metric, T data) {
        metrics.computeIfAbsent(metric, (m) -> new ArrayList<>())
               .add(new DefaultMetricRecord<>(metric, data));
    }

    @Override
    public MetricCollector createChild(String name) {
        MetricCollector child = new DefaultMetricCollector2(name, map, list);
        children.add(child);
        return child;
    }

    @Override
    public MetricCollection collect() {
        List<MetricCollection> collectedChildren = children.stream()
                .map(MetricCollector::collect)
                .collect(Collectors.toList());

        DefaultMetricCollection metricRecords = new DefaultMetricCollection(name, metrics, collectedChildren);

        log.debug(() -> "Collected metrics records: " + metricRecords);
        return metricRecords;
    }

    @Override
    public String toString() {
        return ToString.builder("DefaultMetricCollector")
            .add("metrics", metrics).build();
    }
}
