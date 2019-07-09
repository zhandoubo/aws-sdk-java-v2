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

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.CollectionAttributeConverter.listConverter;
import static software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.MapAttributeConverter.mapConverter;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.InstantAsIntegerAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.IntegerAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.StringAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.BeanAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.BeanSchema;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.annotation.AttributeElementType;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.annotation.Item;
import software.amazon.awssdk.enhanced.dynamodb.converter.string.bundled.StringStringConverter;
import software.amazon.awssdk.enhanced.dynamodb.model.RequestItem;
import software.amazon.awssdk.enhanced.dynamodb.model.ResponseItem;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.testutils.Waiter;
import software.amazon.awssdk.testutils.service.AwsTestBase;
import software.amazon.awssdk.utils.ImmutableMap;

public class EnhancedClientIntegrationTest extends AwsTestBase {
    private static final String TABLE = "books-" + UUID.randomUUID();
    private static final DynamoDbClient dynamo = DynamoDbClient.builder()
                                                               .credentialsProvider(CREDENTIALS_PROVIDER_CHAIN)
                                                               .build();
    private static final DynamoDbAsyncClient dynamoAsync = DynamoDbAsyncClient.builder()
                                                                              .credentialsProvider(CREDENTIALS_PROVIDER_CHAIN)
                                                                              .build();

    @BeforeClass
    public static void setup() {
        try {
            dynamo.createTable(r -> r.tableName(TABLE)
                                     .keySchema(k -> k.attributeName("isbn").keyType(KeyType.HASH))
                                     .attributeDefinitions(a -> a.attributeName("isbn").attributeType(ScalarAttributeType.S))
                                     .provisionedThroughput(t -> t.readCapacityUnits(5L)
                                                                  .writeCapacityUnits(5L)));
        } catch (ResourceInUseException e) {
            // Table already exists. Awesome.
        }

        System.out.println("Waiting for table to be active...");

        Waiter.run(() -> dynamo.describeTable(r -> r.tableName(TABLE)))
              .until(r -> r.table().tableStatus().equals(TableStatus.ACTIVE))
              .orFail();
    }

    @AfterClass
    public static void cleanup() {
        boolean deleted =
                Waiter.run(() -> dynamo.deleteTable(r -> r.tableName(TABLE)))
                      .ignoringException(DynamoDbException.class)
                      .orReturnFalse();

        if (!deleted) {
            System.err.println("Table could not be cleaned up.");
        }
    }

    @Test
    public void getCanReadTheResultOfPut() throws InterruptedException {
        try (DynamoDbEnhancedClient client = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamo).build()) {
            Table books = client.table(TABLE);

            System.out.println("Putting item...");

            books.putItem(requestItem());

            Thread.sleep(5_000);

            System.out.println("Getting item...");

            ResponseItem book = books.getItem(requestItemKey());

            validateResponseItem(book);
        }
    }

    @Test
    public void getCanReadTheResultOfPutAsync() throws InterruptedException {
        try (DynamoDbEnhancedAsyncClient client = DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(dynamoAsync).build()) {
            AsyncTable books = client.table(TABLE);

            System.out.println("Putting item...");

            books.putItem(requestItem()).join();

            Thread.sleep(5_000);

            System.out.println("Getting item...");

            ResponseItem book = books.getItem(requestItemKey()).join();

            validateResponseItem(book);
        }
    }

    @Test
    public void mappedGetCanReadTheResultOfPut() throws InterruptedException {
        BeanSchema<?> bookSchema =
                BeanSchema.builder(Book.class)
                          .constructor(Book::new)

                          .addAttribute(String.class, a ->
                                  a.attributeName("isbn")
                                   .setter(Book::setIsbn)
                                   .getter(Book::getIsbn)
                                   .converter(StringAttributeConverter.create()))

                          .addAttribute(String.class, a ->
                                  a.attributeName("title")
                                   .setter(Book::setTitle)
                                   .getter(Book::getTitle)
                                   .converter(StringAttributeConverter.create()))

                          .addAttribute(TypeToken.listOf(String.class), a ->
                                  a.attributeName("authors")
                                   .setter(Book::setAuthors)
                                   .getter(Book::getAuthors)
                                   .converter(listConverter(StringAttributeConverter.create())))

                          .addAttribute(TypeToken.mapOf(String.class, Instant.class), a ->
                                  a.attributeName("publicationDates")
                                   .setter(Book::setPublicationDates)
                                   .getter(Book::getPublicationDates)
                                   .converter(mapConverter(StringStringConverter.create(),
                                                           InstantAsIntegerAttributeConverter.create())))

                          .addAsymmetricAttribute(String.class, Integer.class, a ->
                                  a.attributeName("asymmetricAttribute")
                                   .getter(Book::getAsymmetricAttribute)
                                   .setter(Book::setAsymmetricAttribute)
                                   .getterConverter(StringAttributeConverter.create())
                                   .setterConverter(IntegerAttributeConverter.create()))

                          .build();

        try (DynamoDbEnhancedClient client =
                     DynamoDbEnhancedClient.builder()
                                           .dynamoDbClient(dynamo)
                                           .addConverter(BeanAttributeConverter.create(bookSchema))
                                           .build()) {
            MappedTable books = client.mappedTable(TABLE);

            System.out.println("Putting item...");

            books.putItem(requestObject());

            Thread.sleep(5_000);

            System.out.println("Getting item...");

            Book book = books.getItem(Book.class, requestObjectKey());

            validateResponseObject(book);
        }
    }

    @Test
    public void mappedGetCanReadTheResultOfPutWithAnnotatedBean() throws InterruptedException {
        try (DynamoDbEnhancedClient client = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamo).build()) {
            MappedTable books = client.mappedTable(TABLE);

            System.out.println("Putting item...");

            books.putItem(annotatedRequestObject());

            Thread.sleep(5_000);

            System.out.println("Getting item...");

            AnnotatedBook book = books.getItem(AnnotatedBook.class, annotatedRequestObjectKey());

            validateResponseObject(book);
        }
    }

    private RequestItem requestItem() {
        return RequestItem.builder()
                          .putAttribute("isbn", "0-330-25864-8")
                          .putAttribute("title", "The Hitchhiker's Guide to the Galaxy")
                          .putAttribute("publicationDates", p -> p.putAttribute("UK", Instant.parse("1979-10-12T00:00:00Z"))
                                                                  .putAttribute("US", Instant.parse("1980-01-01T00:00:00Z")))
                          .putAttribute("authors", Collections.singletonList("Douglas Adams"))
                          .build();
    }

    private RequestItem requestItemKey() {
        return RequestItem.builder()
                          .putAttribute("isbn", "0-330-25864-8")
                          .build();
    }

    private Book requestObject() {
        Book book = new Book();
        book.setIsbn("0-330-25864-8");
        book.setTitle("The Hitchhiker's Guide to the Galaxy");
        book.setPublicationDates(ImmutableMap.of("UK", Instant.parse("1979-10-12T00:00:00Z"),
                                                 "US", Instant.parse("1980-01-01T00:00:00Z")));
        book.setAuthors(Collections.singletonList("Douglas Adams"));
        book.setAsymmetricAttribute(42);
        return book;
    }

    private AnnotatedBook annotatedRequestObject() {
        AnnotatedBook book = new AnnotatedBook();
        book.setIsbn("0-330-25864-8");
        book.setTitle("The Hitchhiker's Guide to the Galaxy");
        book.setPublicationDates(ImmutableMap.of("UK", Instant.parse("1979-10-12T00:00:00Z"),
                                                 "US", Instant.parse("1980-01-01T00:00:00Z")));
        book.setAuthors(Collections.singletonList("Douglas Adams"));
        book.setAsymmetricAttribute(42);
        return book;
    }

    private Book requestObjectKey() {
        Book book = new Book();
        book.setIsbn("0-330-25864-8");
        return book;
    }

    private AnnotatedBook annotatedRequestObjectKey() {
        AnnotatedBook book = new AnnotatedBook();
        book.setIsbn("0-330-25864-8");
        return book;
    }

    private void validateResponseItem(ResponseItem book) {
        Map<String, Instant> publicationDates = new LinkedHashMap<>();
        publicationDates.put("UK", Instant.parse("1979-10-12T00:00:00Z"));
        publicationDates.put("US", Instant.parse("1980-01-01T00:00:00Z"));

        assertThat(book.attribute("isbn").asString()).isEqualTo("0-330-25864-8");
        assertThat(book.attribute("title").asString()).isEqualTo("The Hitchhiker's Guide to the Galaxy");
        assertThat(book.attribute("publicationDates").asMapOf(String.class, Instant.class)).isEqualTo(publicationDates);
        assertThat(book.attribute("authors").asListOf(String.class)).isEqualTo(Collections.singletonList("Douglas Adams"));
    }

    private void validateResponseObject(Book book) {
        Map<String, Instant> publicationDates = new LinkedHashMap<>();
        publicationDates.put("UK", Instant.parse("1979-10-12T00:00:00Z"));
        publicationDates.put("US", Instant.parse("1980-01-01T00:00:00Z"));

        assertThat(book.getIsbn()).isEqualTo("0-330-25864-8");
        assertThat(book.getTitle()).isEqualTo("The Hitchhiker's Guide to the Galaxy");
        assertThat(book.getPublicationDates()).isEqualTo(publicationDates);
        assertThat(book.getAuthors()).isEqualTo(Collections.singletonList("Douglas Adams"));
        assertThat(book.getAsymmetricAttribute()).isEqualTo("42");
    }

    private void validateResponseObject(AnnotatedBook book) {
        Map<String, Instant> publicationDates = new LinkedHashMap<>();
        publicationDates.put("UK", Instant.parse("1979-10-12T00:00:00Z"));
        publicationDates.put("US", Instant.parse("1980-01-01T00:00:00Z"));

        assertThat(book.getIsbn()).isEqualTo("0-330-25864-8");
        assertThat(book.getTitle()).isEqualTo("The Hitchhiker's Guide to the Galaxy");
        assertThat(book.getPublicationDates()).isEqualTo(publicationDates);
        assertThat(book.getAuthors()).isEqualTo(Collections.singletonList("Douglas Adams"));
        assertThat(book.getAsymmetricAttribute()).isEqualTo("42");
    }

    @Item
    public static final class AnnotatedBook {
        private String isbn;
        private String title;
        private Map<String, Instant> publicationDates;
        private List<String> authors;
        private String asymmetricAttribute;

        public String getIsbn() {
            return isbn;
        }

        public void setIsbn(String isbn) {
            this.isbn = isbn;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public Map<String, Instant> getPublicationDates() {
            return publicationDates;
        }

        @AttributeElementType({String.class, Instant.class})
        public void setPublicationDates(Map<String, Instant> publicationDates) {
            this.publicationDates = publicationDates;
        }

        public List<String> getAuthors() {
            return authors;
        }

        @AttributeElementType({String.class})
        public void setAuthors(List<String> authors) {
            this.authors = authors;
        }

        public String getAsymmetricAttribute() {
            return asymmetricAttribute;
        }

        public void setAsymmetricAttribute(Integer asymmetricAttribute) {
            this.asymmetricAttribute = asymmetricAttribute == null ? null : asymmetricAttribute.toString();
        }
    }

    public static final class Book {
        private String isbn;
        private String title;
        private Map<String, Instant> publicationDates;
        private List<String> authors;
        private String asymmetricAttribute;

        public String getIsbn() {
            return isbn;
        }

        public void setIsbn(String isbn) {
            this.isbn = isbn;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public Map<String, Instant> getPublicationDates() {
            return publicationDates;
        }

        public void setPublicationDates(Map<String, Instant> publicationDates) {
            this.publicationDates = publicationDates;
        }

        public List<String> getAuthors() {
            return authors;
        }

        public void setAuthors(List<String> authors) {
            this.authors = authors;
        }

        public String getAsymmetricAttribute() {
            return asymmetricAttribute;
        }

        public void setAsymmetricAttribute(Integer asymmetricAttribute) {
            this.asymmetricAttribute = asymmetricAttribute == null ? null : asymmetricAttribute.toString();
        }
    }
}
