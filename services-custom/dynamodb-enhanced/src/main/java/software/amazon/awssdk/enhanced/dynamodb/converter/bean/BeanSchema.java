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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.model.ItemAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.builder.CopyableBuilder;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

/**
 * A schema that describes the structure of a Java bean.
 *
 * <p>
 * This schema is usually used along with {@link BeanAttributeConverter} to efficiently convert between a bean and a DynamoDB
 * {@link ItemAttributeValue}.
 *
 * <p>
 * This schema does not have the same requirements as a traditional Java bean. Specifically, this only requires that the "bean"
 * be mutable, with a method to retrieve each attribute and a method to set each attribute.
 *
 * <p>
 * A bean attribute may have the same type on its "getter" and "setter" methods ({@link BeanAttribute}), or different types
 * ({@link AsymmetricBeanAttribute}).
 *
 * <p>
 * See {@link BeanAttributeConverter} for usage examples.
 *
 * @param <B> The type of bean represented by this schema.
 *
 * @see BeanAttributeConverter
 */
@SdkPublicApi
@ThreadSafe
@Immutable
public final class BeanSchema<B> implements ToCopyableBuilder<BeanSchema.Builder<B>, BeanSchema<B>> {
    private final TypeToken<B> beanType;
    private final Supplier<? extends B> constructor;
    private final Map<String, AsymmetricBeanAttribute<B, ?, ?>> attributeSchemas;

    private BeanSchema(Builder<B> builder) {
        this.beanType = Validate.paramNotNull(builder.beanType, "beanType");
        this.constructor = Validate.paramNotNull(builder.constructor, "constructor");
        this.attributeSchemas = builder.attributeSchemas.stream().collect(Collectors.toMap(s -> s.attributeName(), s -> s));
    }

    /**
     * Create a {@link BeanSchema.Builder} for the provided bean type.
     */
    public static <B> Builder<B> builder(Class<B> beanType) {
        return new Builder<>(beanType);
    }

    /**
     * Create a {@link BeanSchema.Builder} for the provided bean type.
     */
    public static <B> Builder<B> builder(TypeToken<B> beanType) {
        return new Builder<>(beanType);
    }

    /**
     * Retrieve the type of bean this schema represents.
     */
    public TypeToken<B> beanType() {
        return beanType;
    }

    /**
     * Retrieve the constructor capable of creating an instance of a bean.
     */
    public Supplier<? extends B> constructor() {
        return constructor;
    }

    /**
     * Retrieve the schema of the requested attribute.
     */
    public AsymmetricBeanAttribute<B, ?, ?> attribute(String attributeName) {
        return attributeSchemas.get(attributeName);
    }

    /**
     * Retrieve an unmodifiable collection of the attribute schemas in this bean.
     */
    public Collection<AsymmetricBeanAttribute<B, ?, ?>> attributes() {
        return Collections.unmodifiableCollection(attributeSchemas.values());
    }

    @Override
    public Builder<B> toBuilder() {
        return builder(beanType).constructor(constructor)
                                .addAsymmetricAttributes(attributeSchemas.values());
    }

    /**
     * A builder for configuring and creating a {@link BeanSchema}.
     */
    public static final class Builder<B> implements CopyableBuilder<BeanSchema.Builder<B>, BeanSchema<B>> {
        private final TypeToken<B> beanType;
        private Supplier<? extends B> constructor;
        private Collection<AsymmetricBeanAttribute<B, ?, ?>> attributeSchemas = new ArrayList<>();

        private Builder(Class<B> beanType) {
            this(TypeToken.of(beanType));
        }

        private Builder(TypeToken<B> beanType) {
            this.beanType = beanType;
        }

        /**
         * Specify the constructor that should be used for creating new instances of this bean.
         */
        public Builder<B> constructor(Supplier<? extends B> constructor) {
            this.constructor = constructor;
            return this;
        }

        /**
         * Add a collection of attribute definitions to this bean.
         *
         * <p>
         * This is for attributes where the "get" and "set" method work based on the same types. For attributes with
         * different "get" and "set" types, see {@link #addAsymmetricAttributes(Collection)}.
         */
        public Builder<B> addAttributes(Collection<? extends BeanAttribute<B, ?>> attributeSchemas) {
            Validate.paramNotNull(attributeSchemas, "attributeSchemas");
            Validate.noNullElements(attributeSchemas, "Attribute schemas must not be null.");
            attributeSchemas.stream().map(BeanAttribute::toAsymmetricBeanAttribute).forEach(this.attributeSchemas::add);
            return this;
        }

        /**
         * Add an attribute definition to this bean.
         *
         * <p>
         * This is for attributes where the "get" and "set" method work based on the same types. For attributes with
         * different "get" and "set" types, see {@link #addAsymmetricAttribute(AsymmetricBeanAttribute)}.
         */
        public Builder<B> addAttribute(BeanAttribute<B, ?> attributeSchema) {
            Validate.paramNotNull(attributeSchema, "attributeSchema");
            this.attributeSchemas.add(attributeSchema.toAsymmetricBeanAttribute());
            return this;
        }

        /**
         * Add an attribute definition to this bean. This is a convenience method for fluently invoking
         * {@link #addAttribute(BeanAttribute)}.
         *
         * <p>
         * This is for attributes where the "get" and "set" method work based on the same types. For attributes with
         * different "get" and "set" types, see {@link #addAsymmetricAttribute(Class, Class, Consumer)}
         */
        public <A> Builder<B> addAttribute(Class<A> attributeType,
                                           Consumer<BeanAttribute.Builder<B, A>> attributeConsumer) {
            return addAttribute(TypeToken.of(attributeType), attributeConsumer);
        }

        /**
         * Add an attribute definition to this bean. This is a convenience method for fluently invoking
         * {@link #addAttribute(BeanAttribute)}.
         *
         * <p>
         * This is for attributes where the "get" and "set" method work based on the same types. For attributes with
         * different "get" and "set" types, see {@link #addAsymmetricAttribute(TypeToken, TypeToken, Consumer)}
         */
        public <A> Builder<B> addAttribute(TypeToken<A> attributeType,
                                           Consumer<BeanAttribute.Builder<B, A>> attributeConsumer) {
            Validate.paramNotNull(attributeConsumer, "attributeConsumer");
            BeanAttribute.Builder<B, A> schemaBuilder = BeanAttribute.builder(beanType, attributeType);
            attributeConsumer.accept(schemaBuilder);
            return addAttribute(schemaBuilder.build());
        }

        /**
         * Add a collection of attribute definitions to this bean.
         *
         * <p>
         * This is for attributes with different "get" and "set" method types. For attributes with the same "get" and "set"
         * types, see {@link #addAttributes(Collection)}.
         */
        public Builder<B> addAsymmetricAttributes(Collection<? extends AsymmetricBeanAttribute<B, ?, ?>> attributeSchemas) {
            Validate.paramNotNull(attributeSchemas, "attributeSchemas");
            Validate.noNullElements(attributeSchemas, "Attribute schemas must not be null.");
            this.attributeSchemas.addAll(attributeSchemas);
            return this;
        }

        /**
         * Add an attribute definition to this bean.
         *
         * <p>
         * This is for attributes with different "get" and "set" method types. For attributes with the same "get" and "set"
         * types, see {@link #addAttribute(BeanAttribute)}.
         */
        public Builder<B> addAsymmetricAttribute(AsymmetricBeanAttribute<B, ?, ?> attributeSchema) {
            Validate.paramNotNull(attributeSchema, "attributeSchema");
            this.attributeSchemas.add(attributeSchema);
            return this;
        }

        /**
         * Add an attribute definition to this bean. This is a convenience method for fluently invoking
         * {@link #addAsymmetricAttribute(AsymmetricBeanAttribute)}.
         *
         * <p>
         * This is for attributes with different "get" and "set" method types. For attributes with the same "get" and "set"
         * types, see {@link #addAttribute(Class, Consumer)}.
         */
        public <GetterT, SetterT> Builder<B> addAsymmetricAttribute(
                Class<GetterT> getterAttributeType, 
                Class<SetterT> setterAttributeType, 
                Consumer<AsymmetricBeanAttribute.Builder<B, GetterT, SetterT>> attributeConsumer) {
            return addAsymmetricAttribute(TypeToken.of(getterAttributeType),
                                          TypeToken.of(setterAttributeType),
                                          attributeConsumer);
        }

        /**
         * Add an attribute definition to this bean. This is a convenience method for fluently invoking
         * {@link #addAsymmetricAttribute(AsymmetricBeanAttribute)}.
         *
         * <p>
         * This is for attributes with different "get" and "set" method types. For attributes with the same "get" and "set"
         * types, see {@link #addAttribute(TypeToken, Consumer)}.
         */
        public <GetterT, SetterT> Builder<B> addAsymmetricAttribute(
                TypeToken<GetterT> getterAttributeType,
                TypeToken<SetterT> setterAttributeType,
                Consumer<AsymmetricBeanAttribute.Builder<B, GetterT, SetterT>> attributeConsumer) {
            Validate.paramNotNull(attributeConsumer, "attributeConsumer");
            AsymmetricBeanAttribute.Builder<B, GetterT, SetterT> schemaBuilder =
                    AsymmetricBeanAttribute.builder(beanType, getterAttributeType, setterAttributeType);
            attributeConsumer.accept(schemaBuilder);
            return addAsymmetricAttribute(schemaBuilder.build());
        }

        /**
         * Clear all symmetric and asymmetric attributes configured so far in this schema.
         */
        public Builder<B> clearAttributeSchemas() {
            this.attributeSchemas.clear();
            return this;
        }

        /**
         * Build the schema with the configured values.
         */
        public BeanSchema<B> build() {
            return new BeanSchema<>(this);
        }
    }
}
