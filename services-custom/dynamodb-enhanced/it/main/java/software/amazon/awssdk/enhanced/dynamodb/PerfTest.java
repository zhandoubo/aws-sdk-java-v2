/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.awssdk.enhanced.dynamodb;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.CollectionAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.StringAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.UuidAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.BeanAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.BeanSchema;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.annotation.AttributeElementType;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.annotation.Item;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.ImmutableMap;
import software.amazon.awssdk.utils.StringInputStream;

public class PerfTest {
    private static final DynamoDbClient LLC = DynamoDbClient.builder()
                                                            .httpClient(new TestClient())
                                                            .build();
    private static final MappedTable HLC_HARD_CODED =
            DynamoDbEnhancedClient.builder()
                                  .dynamoDbClient(LLC)
                                  .addConverter(BeanAttributeConverter.create(
                                          BeanSchema.builder(Book.class)
                                                    .constructor(Book::new)
                                                    .addAttribute(UUID.class,
                                                                  a -> a.attributeName("id")
                                                                        .converter(UuidAttributeConverter.create())
                                                                        .getter(Book::getId)
                                                                        .setter(Book::setId))
                                                    .addAttribute(TypeToken.listOf(String.class),
                                                                  a -> a.attributeName("list")
                                                                        .converter(CollectionAttributeConverter.listConverter(StringAttributeConverter.create()))
                                                                        .getter(Book::getList)
                                                                        .setter(Book::setList))
                                                    .build()))
                                  .build()
                                  .mappedTable("foo");

    private static final MappedTable HLC =
            DynamoDbEnhancedClient.builder()
                                  .dynamoDbClient(LLC)
                                  .build()
                                  .mappedTable("foo");


    @Test
    public void comparePerformance() {
        time("No Mapper", this::noMapper);
//        time("Hard Coded", this::hardCoded);
        time("Annotated", this::annotated);
    }

    public void time(String testCase, Runnable runnable) {
        System.out.println("Warming up " + testCase);
        Instant warmupEnd = Instant.now().plusSeconds(60 * 2);
        while (Instant.now().isBefore(warmupEnd)) {
            runnable.run();
        }

        System.out.println("Running " + testCase);
        List<Long> measurements = new ArrayList<>();
        for (int i = 1; i <= 5; ++i) {
            Instant end = Instant.now().plusSeconds(30);
            long ops = 0;
            while (Instant.now().isBefore(end)) {
                runnable.run();
                ++ops;
            }

            long measurement = ops / 30;
            measurements.add(measurement);
            System.out.println(testCase + " " + i + " " + measurement + " ops/sec");
        }

        System.out.println(testCase + " " + measurements.stream().mapToLong(l -> l).average().orElse(0.0) + " average ops/sec");
    }

    private void noMapper() {
        LLC.putItem(r -> r.tableName("foo")
                          .item(ImmutableMap.of("id", AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                                                "list", AttributeValue.builder().l(a -> a.s("foo"), a -> a.s("bar")).build()))
                          .build());
    }

    private void hardCoded() {
        Book test = new Book();
        test.setId(UUID.randomUUID());
        test.setList(Arrays.asList("foo", "bar"));

        HLC_HARD_CODED.putItem(test);
    }

    private void annotated() {
        Book test = new Book();
        test.setId(UUID.randomUUID());
        test.setList(Arrays.asList("foo", "bar"));

        HLC.putItem(test);
    }

    @Item
    public static class Book {
        private UUID id;
        private List<String> list;

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public List<String> getList() {
            return list;
        }

        @AttributeElementType(String.class)
        public void setList(List<String> list) {
            this.list = list;
        }
    }

    private static class TestClient implements SdkHttpClient {
        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new TestRequest();
        }

        @Override
        public void close() {
        }

        private class TestRequest implements ExecutableHttpRequest {
            @Override
            public HttpExecuteResponse call() throws IOException {
                AbortableInputStream is = AbortableInputStream.create(new StringInputStream("{}"), () -> {});
                return HttpExecuteResponse.builder()
                                          .response(SdkHttpResponse.builder()
                                                                   .statusCode(200)
                                                                   .statusText("OK")
                                                                   .putHeader("Content-Type", "application/x-amz-json-1.0")
                                                                   .putHeader("Content-Length", "2")
                                                                   .putHeader("Connection", "keep-alive")
                                                                   .build())
                                          .responseBody(is)
                                          .build();
            }

            @Override
            public void abort() {
            }
        }
    }
}
