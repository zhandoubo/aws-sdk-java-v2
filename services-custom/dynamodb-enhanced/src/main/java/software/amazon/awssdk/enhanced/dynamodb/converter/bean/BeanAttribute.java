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

import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.model.ItemAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.builder.CopyableBuilder;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

/**
 * A schema defining a specific attribute in a bean.
 *
 * <p>
 * This describes how to read and write a specific attribute from a bean as well as how it should be converted
 * to/from an DynamoDB {@link ItemAttributeValue}. This is usually created and added to a {@link BeanSchema}.
 *
 * <p>
 * This is for attributes where the "get" and "set" method work based on the same types. For attributes with
 * different "get" and "set" types, see {@link AsymmetricBeanAttribute}.
 *
 * @param <B> The type of the bean.
 * @param <A> The type of the attribute.
 */
@SdkPublicApi
@ThreadSafe
@Immutable
public final class BeanAttribute<B, A>
        implements ToCopyableBuilder<BeanAttribute.Builder<B, A>, BeanAttribute<B, A>> {
    private final TypeToken<B> beanType;
    private final TypeToken<A> attributeType;
    private final String attributeName;
    private final BeanAttributeGetter<B, A> getter;
    private final BeanAttributeSetter<B, A> setter;
    private final AttributeConverter<A> converter;

    private BeanAttribute(Builder<B, A> builder) {
        this.beanType = Validate.notNull(builder.beanType, "beanType");
        this.attributeType = Validate.paramNotNull(builder.attributeType, "attributeType");
        this.attributeName = Validate.paramNotBlank(builder.attributeName, "attributeName");
        this.getter = Validate.paramNotNull(builder.getter, "getter");
        this.setter = Validate.paramNotNull(builder.setter, "setter");
        this.converter = Validate.paramNotNull(builder.converter, "converter");
    }

    /**
     * Create a builder for configuring and creating bean attributes, using the provided bean and attribute type.
     */
    public static <B, A> Builder<B, A> builder(Class<B> beanType, Class<A> attributeType) {
        return builder(TypeToken.of(beanType), TypeToken.of(attributeType));
    }

    /**
     * Create a builder for configuring and creating bean attributes, using the provided bean and attribute type.
     */
    public static <B, A> Builder<B, A> builder(TypeToken<B> beanType, TypeToken<A> attributeType) {
        return new Builder<>(beanType, attributeType);
    }

    /**
     * Convert this symmetric bean attribute into a {@link AsymmetricBeanAttribute} with the same type on the getter and setter.
     */
    public AsymmetricBeanAttribute<B, A, A> toAsymmetricBeanAttribute() {
        return AsymmetricBeanAttribute.builder(beanType, attributeType, attributeType)
                                      .attributeName(attributeName)
                                      .setter(setter)
                                      .getter(getter)
                                      .setterConverter(converter)
                                      .getterConverter(converter)
                                      .build();
    }

    /**
     * Retrieve the type of this attribute.
     */
    public TypeToken<A> attributeType() {
        return attributeType;
    }

    /**
     * Retrieve the name of this attribute.
     */
    public String attributeName() {
        return attributeName;
    }

    /**
     * Retrieve the getter used to read this attribute's value from the bean.
     */
    public BeanAttributeGetter<B, A> getter() {
        return getter;
    }

    /**
     * Retrieve the setter used to write this attribute's value to the bean.
     */
    public BeanAttributeSetter<B, A> setter() {
        return setter;
    }

    /**
     * Retrieve the converter used to convert this attribute's Java type to/from a DynamoDB {@link ItemAttributeValue}.
     */
    public AttributeConverter<A> converter() {
        return converter;
    }

    @Override
    public Builder<B, A> toBuilder() {
        return builder(beanType, attributeType).attributeName(attributeName)
                                               .getter(getter)
                                               .setter(setter)
                                               .converter(converter);
    }

    /**
     * A builder for configuring and creating a {@link BeanAttribute}.
     */
    public static final class Builder<B, A> implements CopyableBuilder<Builder<B, A>, BeanAttribute<B, A>> {
        private final TypeToken<B> beanType;
        private final TypeToken<A> attributeType;
        private String attributeName;
        private BeanAttributeGetter<B, A> getter;
        private BeanAttributeSetter<B, A> setter;
        private AttributeConverter<A> converter;

        private Builder(TypeToken<B> beanType, TypeToken<A> attributeType) {
            this.beanType = beanType;
            this.attributeType = attributeType;
        }

        /**
         * Configure the name of this attribute, as it should be stored in DynamoDB.
         */
        public Builder<B, A> attributeName(String attributeName) {
            this.attributeName = attributeName;
            return this;
        }

        /**
         * Configure the method used to read the value of this attribute from the bean.
         */
        public Builder<B, A> getter(BeanAttributeGetter<B, A> getter) {
            this.getter = getter;
            return this;
        }

        /**
         * Configure the method used to write the value of this attribute to the bean.
         */
        public Builder<B, A> setter(BeanAttributeSetter<B, A> setter) {
            this.setter = setter;
            return this;
        }

        /**
         * Retrieve the converter used to convert this attribute's Java type to/from a DynamoDB {@link ItemAttributeValue}.
         */
        public Builder<B, A> converter(AttributeConverter<A> converter) {
            this.converter = converter;
            return this;
        }

        public BeanAttribute<B, A> build() {
            return new BeanAttribute<>(this);
        }
    }
}
