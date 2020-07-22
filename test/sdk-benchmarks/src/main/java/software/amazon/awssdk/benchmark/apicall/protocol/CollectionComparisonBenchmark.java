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

package software.amazon.awssdk.benchmark.apicall.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.MetricLevel;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.metrics.internal.DefaultMetricCollector;
import software.amazon.awssdk.metrics.internal.DefaultMetricCollector2;

/**
 * Benchmarking for running with different protocols.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class CollectionComparisonBenchmark {
    private static final List<SdkMetric<Integer>> METRICS;

    static {
        METRICS = new ArrayList<>();
        for (int i = 0; i < 25; ++i) {
            METRICS.add(SdkMetric.create("metric-" + i, Integer.class, MetricLevel.INFO, MetricCategory.CORE));
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"SYNCHRONIZED_MAP", "CONCURRENT_MAP", "CONCURRENT_SKIP_LIST_MAP"})
        private MapType mapType;

        @Param({"SYNCHRONIZED_LIST", "LINKED_BLOCKING_QUEUE", "CONCURRENT_LINKED_QUEUE"})
        private ListType listType;
    }

    public static enum MapType {
        SYNCHRONIZED_MAP(() -> Collections.synchronizedMap(new HashMap<>())),
        CONCURRENT_MAP(ConcurrentHashMap::new),
        CONCURRENT_SKIP_LIST_MAP(ConcurrentSkipListMap::new);

        private Supplier<Map<SdkMetric<?>, List<MetricRecord<?>>>> map;

        private MapType(Supplier<Map<SdkMetric<?>, List<MetricRecord<?>>>> map) {
            this.map = map;
        }
    }

    public static enum ListType {
        SYNCHRONIZED_LIST(() -> Collections.synchronizedList(new ArrayList<>())),
        LINKED_BLOCKING_QUEUE(LinkedBlockingQueue::new),
        CONCURRENT_LINKED_QUEUE(ConcurrentLinkedQueue::new);

        private Supplier<Collection<MetricCollector>> list;

        private ListType(Supplier<Collection<MetricCollector>> list) {
            this.list = list;
        }
    }

    @Benchmark
    public void statusQuoSerialWriteThenCollect(Blackhole blackhole) {
        benchmarkCollector(blackhole, new DefaultMetricCollector("test"));
    }

    @Benchmark
    public void serialWriteThenCollect(Blackhole blackhole, BenchmarkState state) {
        benchmarkCollector(blackhole, new DefaultMetricCollector2("test", state.mapType.map, state.listType.list));
    }

    private void benchmarkCollector(Blackhole blackhole, MetricCollector collector) {
        METRICS.forEach(m -> collector.reportMetric(m, 0));
        blackhole.consume(collector.collect());
    }

    public static void main(String... args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(CollectionComparisonBenchmark.class.getSimpleName())
            .build();
        new Runner(opt).run();
    }
}
