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

package software.amazon.awssdk.enhanced.dynamodb.internal;

import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.MappedTable;
import software.amazon.awssdk.enhanced.dynamodb.Table;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.ConversionContext;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.SubtypeAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.model.ItemAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.builder.Buildable;

/**
 * The default implementation of {@link Table}.
 */
@SdkInternalApi
@ThreadSafe
public class DefaultMappedTable implements MappedTable {
    private final DynamoDbClient client;
    private final String tableName;
    private final SubtypeAttributeConverter<Object> converter;

    private DefaultMappedTable(Builder builder) {
        this.client = builder.client;
        this.tableName = builder.tableName;
        this.converter = builder.converter;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public <T extends U, U> T getItem(Class<T> outputType, U key) {
        GetItemResponse response = client.getItem(r -> r.tableName(tableName)
                                                        .key(convertToGeneratedItem(key)));

        ConversionContext context = ConversionContext.builder().attributeConverter(converter).build();

        Object output = converter.fromAttributeValue(ItemAttributeValue.fromGeneratedItem(response.item()),
                                                     TypeToken.of(outputType),
                                                     context);

        return Validate.isInstanceOf(outputType, output,
                                     "Converter generated a %s when a %s was requested.", output.getClass(), outputType);
    }

    @Override
    public void putItem(Object item) {
        client.putItem(r -> r.tableName(tableName)
                             .item(convertToGeneratedItem(item)));
    }

    private Map<String, AttributeValue> convertToGeneratedItem(Object item) {
        ConversionContext context = ConversionContext.builder().attributeConverter(converter).build();
        ItemAttributeValue itemAsAttributeValue = converter.toAttributeValue(item, context);

        Validate.isTrue(itemAsAttributeValue.isMap(),
                        "Input type %s was converted to a %s, but only MAP is supported by a MappedTable. Update your " +
                        "converters to one that supports converting this input type to a MAP.",
                        item.getClass(), itemAsAttributeValue.type());

        return itemAsAttributeValue.toGeneratedItem();
    }


    public static class Builder implements Buildable {
        private String tableName;
        private DynamoDbClient client;
        private SubtypeAttributeConverter<Object> converter;

        public Builder name(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder dynamoDbClient(DynamoDbClient client) {
            this.client = client;
            return this;
        }

        public Builder converter(SubtypeAttributeConverter<Object> converter) {
            this.converter = converter;
            return this;
        }

        @Override
        public DefaultMappedTable build() {
            return new DefaultMappedTable(this);
        }
    }
}
