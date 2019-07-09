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

package software.amazon.awssdk.enhanced.dynamodb.converter.bean;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.ConversionContext;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.SubtypeAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.UnionAttributeConverter.Visitor;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.annotation.AnnotatedBeanAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.model.ItemAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeConvertingVisitor;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;
import software.amazon.awssdk.utils.Validate;

/**
 * A converter between a specific Bean type and an {@link ItemAttributeValue}.
 *
 * <p>
 * This is the best performing bean-to-attribute-value converter available. This requires the user to define a {@link BeanSchema}
 * that directly instructs the enhanced client how to create, populate and convert every field in the Bean.
 *
 * <p>
 * This is usually used when a {@link DynamoDbEnhancedClient} is created:
 * <pre>
 *     class User {
 *         private String id;
 *         private Instant creationTime;
 *
 *         public String getId() { return id; }
 *         public void setId(String id) { this.id = id; }
 *         public Instant getCreationTime() { return creationTime; }
 *         public void setCreationTime(Instant creationTime) { this.creationTime = creationTime; }
 *     }
 *
 *     BeanSchema<User> userSchema =
 *             BeanSchema.builder(User.class)
 *                       .constructor(User::new)
 *                       .addAttribute(String.class, a ->
 *                               a.attributeName("id")
 *                                .getter(User::getId)
 *                                .setter(User::setId)
 *                                .converter(StringAttributeConverter.create()))
 *                       .addAttribute(Instant.class, a ->
 *                               a.attributeName("title")
 *                                .setter(User::setCreationTime)
 *                                .getter(User::getCreationTime)
 *                                .converter(InstantAsIntegerAttributeConverter.create()))
 *                       .build();
 *
 *     BeanAttributeConverter<User> converter = BeanAttributeConverter.creation(userSchema);
 *
 *     try (DynamoDbEnhancedClient client = DynamoDbEnhancedClient.builder()
 *                                                                .addConverter(converter)
 *                                                                .build()) {
 *         MappedTable users = client.mappedTable("users");
 *         User user = new User();
 *         user.setId("1");
 *         user.setCreationTime(Instant.now());
 *
 *         users.putItem(users);
 *     }
 * </pre>
 *
 * @param <T> The type of bean supported by this converter.
 *
 * @see AnnotatedBeanAttributeConverter
 */
@SdkPublicApi
@ThreadSafe
@Immutable
public class BeanAttributeConverter<T> implements AttributeConverter<T> {
    private final BeanSchema<T> schema;

    private BeanAttributeConverter(BeanSchema<T> schema) {
        this.schema = schema;
    }

    /**
     * Create a converter for the provided {@link BeanSchema}.
     */
    public static <T> BeanAttributeConverter<T> create(BeanSchema<T> schema) {
        return new BeanAttributeConverter<>(schema);
    }

    @Override
    public TypeToken<T> type() {
        return schema.beanType();
    }

    @Override
    public ItemAttributeValue toAttributeValue(T input, ConversionContext context) {
        Map<String, ItemAttributeValue> mappedValues = new LinkedHashMap<>();
        schema.attributes().forEach(attr -> mapAttribute(input, context, attr)
                .ifPresent(mappedValue -> mappedValues.put(attr.attributeName(), mappedValue)));
        return ItemAttributeValue.fromMap(mappedValues);
    }

    private <GetterT> Optional<ItemAttributeValue> mapAttribute(T bean,
                                                                ConversionContext context,
                                                                AsymmetricBeanAttribute<T, GetterT, ?> attributeSchema) {
        GetterT attribute = attributeSchema.getter().apply(bean);

        if (attribute == null) {
            return Optional.empty();
        }

        ConversionContext newContext = context.toBuilder()
                                              .attributeName(attributeSchema.attributeName())
                                              .build();

        return Optional.of(attributeSchema.getterConverter().visit(new Visitor<GetterT, ItemAttributeValue>() {
            @Override
            public ItemAttributeValue visit(AttributeConverter<GetterT> converter) {
                return converter.toAttributeValue(attribute, newContext);
            }

            @Override
            public ItemAttributeValue visit(SubtypeAttributeConverter<? super GetterT> converter) {
                return converter.toAttributeValue(attribute, newContext);
            }
        }));
    }

    @Override
    public T fromAttributeValue(ItemAttributeValue input, ConversionContext context) {
        return input.convert(new TypeConvertingVisitor<T>(schema.beanType().rawClass(), BeanAttributeConverter.class) {
            @Override
            public T convertMap(Map<String, ItemAttributeValue> value) {
                T response = schema.constructor().get();

                Validate.isInstanceOf(targetType, response,
                                      "Item constructor created a %s, but a %s was requested.",
                                      response.getClass(), targetType);

                schema.attributes().forEach(attributeSchema -> {
                    ItemAttributeValue mappedValue = value.get(attributeSchema.attributeName());
                    convertAndSet(mappedValue, response, attributeSchema);
                });

                return response;
            }

            private <SetterT> void convertAndSet(ItemAttributeValue mappedValue,
                                                 T response,
                                                 AsymmetricBeanAttribute<T, ?, SetterT> attributeSchema) {
                ConversionContext newContext = context.toBuilder()
                                                      .attributeName(attributeSchema.attributeName())
                                                      .build();
                SetterT unmappedValue = mappedValue == null ? null :
                        attributeSchema.setterConverter().visit(new Visitor<SetterT, SetterT>() {
                            @Override
                            public SetterT visit(AttributeConverter<SetterT> converter) {
                                return converter.fromAttributeValue(mappedValue, newContext);
                            }

                            @Override
                            public SetterT visit(SubtypeAttributeConverter<? super SetterT> converter) {
                                return converter.fromAttributeValue(mappedValue,
                                                                    attributeSchema.setterAttributeType(),
                                                                    newContext);
                            }
                        });

                attributeSchema.setter().accept(response, attributeSchema.setterAttributeType().rawClass().cast(unmappedValue));
            }
        });
    }
}
