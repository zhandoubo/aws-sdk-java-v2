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
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.SubtypeAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.UnionAttributeConverter;
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
 * This is for attributes where the "get" and "set" method work with different types. For attributes with
 * the same "get" and "set" types, see {@link BeanAttribute}.
 *
 * @param <BeanT> The type of the bean.
 * @param <GetterT> The type returned by the attribute's "getter".
 * @param <SetterT> The type returned by the attribute's "setter".
 */
@SdkPublicApi
@ThreadSafe
@Immutable
public class AsymmetricBeanAttribute<BeanT, GetterT, SetterT>
        implements ToCopyableBuilder<AsymmetricBeanAttribute.Builder<BeanT, GetterT, SetterT>,
        AsymmetricBeanAttribute<BeanT, GetterT, SetterT>> {
    private final TypeToken<BeanT> beanType;
    private final String attributeName;
    private final TypeToken<GetterT> getterAttributeType;
    private final TypeToken<SetterT> setterAttributeType;
    private final BeanAttributeGetter<BeanT, GetterT> getter;
    private final BeanAttributeSetter<BeanT, SetterT> setter;
    private final UnionAttributeConverter<GetterT> getterConverter;
    private final UnionAttributeConverter<SetterT> setterConverter;

    private AsymmetricBeanAttribute(Builder<BeanT, GetterT, SetterT> builder) {
        this.beanType = Validate.notNull(builder.beanType, "beanType");
        this.attributeName = Validate.paramNotBlank(builder.attributeName, "attributeName");
        this.getterAttributeType = Validate.paramNotNull(builder.getterAttributeType, "getterAttributeType");
        this.setterAttributeType = Validate.paramNotNull(builder.setterAttributeType, "setterAttributeType");
        this.getter = Validate.paramNotNull(builder.getter, "getter");
        this.setter = Validate.paramNotNull(builder.setter, "setter");
        this.getterConverter = Validate.paramNotNull(builder.getterConverter, "getterConverter");
        this.setterConverter = Validate.paramNotNull(builder.setterConverter, "setterConverter");
    }

    /**
     * Create a builder for configuring and creating bean attributes, using the provided bean and attribute types.
     */
    public static <BeanT, GetterT, SetterT> Builder<BeanT, GetterT, SetterT> builder(Class<BeanT> beanType,
                                                                                     Class<GetterT> getterAttributeType,
                                                                                     Class<SetterT> setterAttributeType) {
        return builder(TypeToken.of(beanType), TypeToken.of(getterAttributeType), TypeToken.of(setterAttributeType));
    }

    /**
     * Create a builder for configuring and creating bean attributes, using the provided bean and attribute types.
     */
    public static <BeanT, GetterT, SetterT> Builder<BeanT, GetterT, SetterT> builder(TypeToken<BeanT> beanType,
                                                                                     TypeToken<GetterT> getterAttributeType,
                                                                                     TypeToken<SetterT> setterAttributeType) {
        return new Builder<>(beanType, getterAttributeType, setterAttributeType);
    }

    /**
     * Retrieve the name of this attribute.
     */
    public String attributeName() {
        return attributeName;
    }

    /**
     * Retrieve the type returned by this attribute's getter.
     */
    public TypeToken<GetterT> getterAttributeType() {
        return getterAttributeType;
    }

    /**
     * Retrieve the type accepted by this attribute's setter.
     */
    public TypeToken<SetterT> setterAttributeType() {
        return setterAttributeType;
    }

    /**
     * Retrieve the getter used to read this attribute's value from the bean.
     */
    public BeanAttributeGetter<BeanT, GetterT> getter() {
        return getter;
    }

    /**
     * Retrieve the setter used to write this attribute's value to the bean.
     */
    public BeanAttributeSetter<BeanT, SetterT> setter() {
        return setter;
    }

    /**
     * Retrieve the converter used to convert this attribute's Java type to a DynamoDB {@link ItemAttributeValue}.
     */
    public UnionAttributeConverter<GetterT> getterConverter() {
        return getterConverter;
    }

    /**
     * Retrieve the converter used to convert a DynamoDB {@link ItemAttributeValue} to this attribute's Java type.
     */
    public UnionAttributeConverter<SetterT> setterConverter() {
        return setterConverter;
    }

    @Override
    public Builder<BeanT, GetterT, SetterT> toBuilder() {
        return builder(beanType, getterAttributeType, setterAttributeType)
                .attributeName(attributeName)
                .getter(getter)
                .setter(setter)
                .getterConverter(getterConverter)
                .setterConverter(setterConverter);
    }

    /**
     * A builder for configuring and creating a {@link BeanAttribute}.
     */
    public static final class Builder<BeanT, GetterT, SetterT>
            implements CopyableBuilder<Builder<BeanT, GetterT, SetterT>, AsymmetricBeanAttribute<BeanT, GetterT, SetterT>> {
        private TypeToken<BeanT> beanType;
        private String attributeName;
        private TypeToken<GetterT> getterAttributeType;
        private TypeToken<SetterT> setterAttributeType;
        private BeanAttributeGetter<BeanT, GetterT> getter;
        private BeanAttributeSetter<BeanT, SetterT> setter;
        private UnionAttributeConverter<GetterT> getterConverter;
        private UnionAttributeConverter<SetterT> setterConverter;

        private Builder(TypeToken<BeanT> beanType,
                        TypeToken<GetterT> getterAttributeType,
                        TypeToken<SetterT> setterAttributeType) {
            this.beanType = beanType;
            this.getterAttributeType = getterAttributeType;
            this.setterAttributeType = setterAttributeType;
        }

        /**
         * Configure the name of this attribute, as it should be stored in DynamoDB.
         */
        public Builder<BeanT, GetterT, SetterT> attributeName(String attributeName) {
            this.attributeName = attributeName;
            return this;
        }

        /**
         * Configure the method used to read the value of this attribute from the bean.
         */
        public Builder<BeanT, GetterT, SetterT> getter(BeanAttributeGetter<BeanT, GetterT> getter) {
            this.getter = getter;
            return this;
        }

        /**
         * Configure the method used to write the value of this attribute to the bean.
         */
        public Builder<BeanT, GetterT, SetterT> setter(BeanAttributeSetter<BeanT, SetterT> setter) {
            this.setter = setter;
            return this;
        }

        /**
         * Configure the converter used to convert this attribute's Java type to a DynamoDB {@link ItemAttributeValue}.
         */
        public Builder<BeanT, GetterT, SetterT> getterConverter(AttributeConverter<GetterT> getterConverter) {
            this.getterConverter = UnionAttributeConverter.create(getterConverter);
            return this;
        }

        /**
         * Configure the converter used to convert this attribute's Java type to a DynamoDB {@link ItemAttributeValue}.
         */
        public Builder<BeanT, GetterT, SetterT> getterConverter(SubtypeAttributeConverter<? super GetterT> getterConverter) {
            this.getterConverter = UnionAttributeConverter.create(getterConverter);
            return this;
        }

        /**
         * Configure the converter used to convert this attribute's Java type to a DynamoDB {@link ItemAttributeValue}.
         */
        public Builder<BeanT, GetterT, SetterT> getterConverter(UnionAttributeConverter<GetterT> getterConverter) {
            this.getterConverter = getterConverter;
            return this;
        }

        /**
         * Retrieve the converter used to convert a DynamoDB {@link ItemAttributeValue} to this attribute's Java type.
         */
        public Builder<BeanT, GetterT, SetterT> setterConverter(AttributeConverter<SetterT> setterConverter) {
            this.setterConverter = UnionAttributeConverter.create(setterConverter);
            return this;
        }

        /**
         * Retrieve the converter used to convert a DynamoDB {@link ItemAttributeValue} to this attribute's Java type.
         */
        public Builder<BeanT, GetterT, SetterT> setterConverter(SubtypeAttributeConverter<? super SetterT> setterConverter) {
            this.setterConverter = UnionAttributeConverter.create(setterConverter);
            return this;
        }

        /**
         * Retrieve the converter used to convert a DynamoDB {@link ItemAttributeValue} to this attribute's Java type.
         */
        public Builder<BeanT, GetterT, SetterT> setterConverter(UnionAttributeConverter<SetterT> setterConverter) {
            this.setterConverter = setterConverter;
            return this;
        }

        public AsymmetricBeanAttribute<BeanT, GetterT, SetterT> build() {
            return new AsymmetricBeanAttribute<>(this);
        }
    }
}
