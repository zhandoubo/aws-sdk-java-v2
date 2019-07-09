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

package software.amazon.awssdk.enhanced.dynamodb.converter.bean.annotation;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.MappedTable;
import software.amazon.awssdk.enhanced.dynamodb.Table;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.ConversionContext;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.SubtypeAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled.DefaultAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.bean.BeanAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.annotation.AnnotatedBean;
import software.amazon.awssdk.enhanced.dynamodb.model.ItemAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;

/**
 * A converter between Java beans that are annotated with {@link Item} and {@link ItemAttributeValue}s.
 *
 * <p>
 * <b>Usage Overview</b>
 *
 * <p>
 * This convert is included in the {@link DefaultAttributeConverter}. Any objects not processed by other converters in the chain
 * will be processed by this converter, by default.
 *
 * <p>
 * To use this converter, annotate your bean with {@link Item} and use it inside of a {@link Table} or {@link MappedTable}
 * request:
 * <pre>
 *     {@literal @}Item
 *     public class User {
 *         private String id;
 *         private Instant creationTime;
 *
 *         public String getId() { return id; }
 *         public void setId(String id) { this.id = id; }
 *
 *         public Instant getCreationTime() { return creationTime; }
 *         public void setCreationTime(Instant creationTime) { this.creationTime = creationTime; }
 *     }
 *
 *     try (DynamoDbEnhancedClient client = DynamoDbEnhancedClient.create()) {
 *         MappedTable users = client.mappedTable("users");
 *         User user = new User();
 *         user.setId("1");
 *         user.setCreationTime(Instant.now());
 *
 *         users.putItem(users);
 *     }
 * </pre>
 *
 * <p>
 * <b>Support for Collections and Maps</b>
 *
 * <p>
 * Extra care is required when using collections and maps in a bean. Because Java does not preserve type parameters (e.g. the
 * {@code String} in {@code List<String>}), these type parameters must be explicitly called out by adding the
 * {@link AttributeElementType} annotation to the "get" or "set" method:
 *
 * <pre>
 *     {@literal @}Item
 *     public class User {
 *         private String id;
 *         private List<String> aliases;
 *         private Map<Instant, LoginAttempt> logins;
 *
 *         public String getId() { return id; }
 *         public void setId(String id) { this.id = id; }
 *
 *         {@literal @}AttributeElementType(String.class)
 *         public List<String> getCreationTime() { return creationTime; }
 *         public void setCreationTime(List<String> aliases) { this.creationTime = creationTime; }
 *
 *         {@literal @}AttributeElementType({Instant.class, LoginAttempt.class})
 *         public Map<Instant, LoginAttempt> getLoginAttempts() { return loginAttempts; }
 *         public void setLoginAttempts(Map<Instant, LoginAttempt> loginAttempts) { this.loginAttempts = loginAttempts; }
 *     }
 *
 *     {@literal @}Item
 *     public class LoginAttempt {
 *         private String sourceIp;
 *
 *         public String getSourceIp() { return sourceIp; }
 *         public void setSourceIp(String sourceIp) { this.sourceIp = sourceIp; }
 *     }
 * </pre>
 *
 * <p>
 * This setting is only used for converting DynamoDB responses to beans. For converting beans to DynamoDB requests, the actual
 * runtime type of the element in the collection is used.
 *
 * <p>
 * <b>Renaming Attributes</b>
 *
 * <p>
 * If the attribute name in the setter (e.g. "id" in "getId") differs from the attribute name in DynamoDB, the DynamoDB attribute
 * name may be specified by adding the {@link AttributeName} annotation to the "get" or "set" method:
 *
 * <pre>
 *     {@literal @}Item
 *     public class User {
 *         private String id;
 *
 *         {@literal @}AttributeName("userId")
 *         public String getId() { return id; }
 *         public void setId(String id) { this.id = id; }
 *     }
 * </pre>
 *
 * <p>
 * <b>Bean Specifics</b>
 *
 * <p>
 * A bean annotated with @Item does not need to strictly follow the Java bean standard. The following requirements are expected:
 * <ol>
 *     <li>The bean itself must be static, public and accessible (public).</li>
 *     <li>The bean must have a zero-argument, public and accessible constructor.</li>
 *     <li>Each "get" and "set" method must be public and accessible.</li>
 *     <li>Each "get" method may instead be specified with an "is" method.</li>
 * </ol>
 *
 * <p>
 * Specifically, the following are *not* requirements:
 * <ol>
 *     <li>That each "get" and "set" method have the same type. The type returned by "get" is used for requests, and the type
 *     accepted by "set" is used for responses. Any uses of the {@link AttributeElementType} annotation should describe the
 *     type parameters of the *set* method. The actual runtime type is used for the type parameters of the "get" method.</li>
 *     <li>That "is" can only be used for {@link Boolean} types. "is" can be used in place of "get" for any type.</li>
 * </ol>
 *
 * <p>
 * <b>Performance</b>
 *
 * <p>
 * This dynamically generates a {@link BeanAttributeConverter} for a bean the first the bean is encountered in a
 * {@link #toAttributeValue(Object, ConversionContext)} or
 * {@link #fromAttributeValue(ItemAttributeValue, TypeToken, ConversionContext)} call. The result of this generation is then
 * cached for all future invocations.
 *
 * <p>
 * After the {@code BeanAttributeConverter} is cached, {@link LambdaMetafactory} and {@link MethodHandle}s are used for accessing
 * constructors and bean methods. {@link SecurityManager} checks for access are performed once, during
 * {@code BeanAttributeConverter} creation. Constructor and bean method invocations are not subject to security checks.
 *
 * <p>
 * Applications with strict cold-start requirements should consider creating a {@code BeanAttributeConverter} at
 * startup to remove the initial reflection cost of analyzing the bean structure. For long-running applications, this
 * converter will have reasonably close performance to {@code BeanAttributeConverter}, without its initialization requirements.
 */
@SdkPublicApi
@Immutable
@ThreadSafe
public class AnnotatedBeanAttributeConverter implements SubtypeAttributeConverter<Object> {
    private ConcurrentMap<Class<?>, BeanAttributeConverter<?>> STATIC_CONVERTER_CACHE = new ConcurrentHashMap<>();

    private AnnotatedBeanAttributeConverter() {}

    public static AnnotatedBeanAttributeConverter create() {
        return new AnnotatedBeanAttributeConverter();
    }

    @Override
    public TypeToken<Object> type() {
        return TypeToken.of(Object.class);
    }

    @Override
    public ItemAttributeValue toAttributeValue(Object input, ConversionContext context) {
        Class<?> inputClass = input.getClass();
        BeanAttributeConverter<?> converter = STATIC_CONVERTER_CACHE.get(inputClass);
        if (converter != null) {
            return toAttributeValue(converter, input, context);
        }

        return toAttributeValue(cacheAndGetBeanConverter(inputClass), input, context);
    }

    private <T> ItemAttributeValue toAttributeValue(BeanAttributeConverter<T> converter,
                                                    Object input,
                                                    ConversionContext context) {
        T castInput = converter.type().rawClass().cast(input);
        return converter.toAttributeValue(castInput, context);
    }

    @Override
    public <U> U fromAttributeValue(ItemAttributeValue input, TypeToken<U> desiredType, ConversionContext context) {
        BeanAttributeConverter<U> converter = (BeanAttributeConverter<U>) STATIC_CONVERTER_CACHE.get(desiredType.rawClass());
        if (converter != null) {
            return converter.fromAttributeValue(input, context);
        }

        return cacheAndGetBeanConverter(desiredType.rawClass()).fromAttributeValue(input, context);
    }

    private <T> BeanAttributeConverter<T> cacheAndGetBeanConverter(Class<T> beanClass) {
        BeanAttributeConverter<T> converter = BeanAttributeConverter.create(AnnotatedBean.create(beanClass).toBeanSchema());
        STATIC_CONVERTER_CACHE.put(beanClass, converter);
        return converter;
    }
}
