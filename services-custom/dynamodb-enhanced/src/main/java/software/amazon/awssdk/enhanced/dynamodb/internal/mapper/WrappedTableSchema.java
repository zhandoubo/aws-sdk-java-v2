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

package software.amazon.awssdk.enhanced.dynamodb.internal.mapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.TableMetadata;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@SdkInternalApi
public abstract class WrappedTableSchema<T, R extends TableSchema<T>> implements TableSchema<T> {
    private final R wrappedTableSchema;

    protected WrappedTableSchema(R wrappedTableSchema) {
        this.wrappedTableSchema = wrappedTableSchema;
    }

    @Override
    public T mapToItem(Map<String, AttributeValue> attributeMap) {
        return this.wrappedTableSchema.mapToItem(attributeMap);
    }

    @Override
    public Map<String, AttributeValue> itemToMap(T item, boolean ignoreNulls) {
        return this.wrappedTableSchema.itemToMap(item, ignoreNulls);
    }

    @Override
    public Map<String, AttributeValue> itemToMap(T item, Collection<String> attributes) {
        return this.wrappedTableSchema.itemToMap(item, attributes);
    }

    @Override
    public AttributeValue attributeValue(T item, String attributeName) {
        return this.wrappedTableSchema.attributeValue(item, attributeName);
    }

    @Override
    public TableMetadata tableMetadata() {
        return this.wrappedTableSchema.tableMetadata();
    }

    @Override
    public EnhancedType<T> itemType() {
        return this.wrappedTableSchema.itemType();
    }

    @Override
    public List<String> attributeNames() {
        return this.wrappedTableSchema.attributeNames();
    }

    @Override
    public boolean isAbstract() {
        return this.wrappedTableSchema.isAbstract();
    }
}
