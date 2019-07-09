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

package software.amazon.awssdk.enhanced.dynamodb.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.Table;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.AttributeConverter;
import software.amazon.awssdk.utils.Validate;

/**
 * An {@link ItemAttributeValue} that can be converted into any Java type.
 *
 * <p>
 * This is usually returned by the SDK from methods that load data from DynamoDB, like {@link Table#getItem(RequestItem)}'s
 * {@link ResponseItem} result.
 *
 * <p>
 * Multiple categories of methods are exposed:
 * <ol>
 *     <li>{@code asType()} methods like {@link #asString()} and {@link #asInteger()} that can be used to retrieve the attribute
 *     value as a type that is definitely supported by the SDK by default. These types will always be supported by the SDK,
 *     unless the converters have been overridden or the value in DynamoDB cannot be converted to the requested type.</li>
 *     <li>{@code as()} methods like {@link #as(Class)} and {@link #as(TypeToken)} that can be used to retrieve the attribute
 *     value as any Java type. These types may be supported by the SDK or custom converters.</li>
 *     <li>{@link #attributeValue()}, which returns the {@link ItemAttributeValue} exactly as it was returned by DynamoDB.</li>
 * </ol>
 */
@SdkPublicApi
@ThreadSafe
@Immutable
public interface ConvertibleItemAttributeValue {
    /**
     * Retrieve the {@link ItemAttributeValue} exactly as it was returned by DynamoDB.
     *
     * <p>
     * This call should never fail with an {@link Exception}.
     */
    ItemAttributeValue attributeValue();

    /**
     * Convert this attribute value into the provided type, using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * For parameterized types, use {@link #as(TypeToken)}.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided class is null.</li>
     *     <li>A converter does not exist for the requested type.</li>
     *     <li>This attribute value cannot be converted to the requested type.</li>
     * </ol>
     */
    <T> T as(Class<T> type);

    /**
     * Convert this attribute value into a type that matches the provided type token.
     *
     * <p>
     * This is useful for parameterized types. Non-parameterized types should use {@link #as(Class)}. Lists should use
     * {@link #asListOf(Class)}. Maps should use {@link #asMapOf(Class, Class)}.
     *
     * <p>
     * When creating a {@link TypeToken}, you must create an anonymous sub-class, e.g.
     * {@code new TypeToken<Collection<String>>()&#123;&#125;} (note the extra {}).
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided token is null.</li>
     *     <li>A converter does not exist for the provided type.</li>
     *     <li>This attribute value cannot be converted to the requested type.</li>
     * </ol>
     */
    <T> T as(TypeToken<T> type);

    /**
     * Convert this attribute value into a {@link BigDecimal} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a BigDecimal.</li>
     * </ol>
     */
    default BigDecimal asBigDecimal() {
        return as(BigDecimal.class);
    }

    /**
     * Convert this attribute value into a {@link BigInteger} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a BigInteger.</li>
     * </ol>
     */
    default BigInteger asBigInteger() {
        return as(BigInteger.class);
    }

    /**
     * Convert this attribute value into a {@link Boolean} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a Boolean.</li>
     * </ol>
     */
    default Boolean asBoolean() {
        return as(Boolean.class);
    }

    /**
     * Convert this attribute value into a {@code byte[]} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a byte[].</li>
     * </ol>
     */
    default SdkBytes asBytes() {
        return as(SdkBytes.class);
    }

    /**
     * Convert this attribute value into a {@link Double} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a Double.</li>
     * </ol>
     */
    default Double asDouble() {
        return as(Double.class);
    }

    /**
     * Convert this attribute value into a {@link Duration} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a Duration.</li>
     * </ol>
     */
    default Duration asDuration() {
        return as(Duration.class);
    }

    /**
     * Convert this attribute value into a {@link Float} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a Float.</li>
     * </ol>
     */
    default Float asFloat() {
        return as(Float.class);
    }

    /**
     * Convert this attribute value into an {@link Integer} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to an Instant.</li>
     * </ol>
     */
    default Instant asInstant() {
        return as(Instant.class);
    }

    /**
     * Convert this attribute value into an {@link Integer} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to an Integer.</li>
     * </ol>
     */
    default Integer asInteger() {
        return as(Integer.class);
    }

    /**
     * Convert this attribute value into a {@link LocalDate} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a LocalDate.</li>
     * </ol>
     */
    default LocalDate asLocalDate() {
        return as(LocalDate.class);
    }

    /**
     * Convert this attribute value into a {@link LocalDateTime} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a LocalDate.</li>
     * </ol>
     */
    default LocalDateTime asLocalDateTime() {
        return as(LocalDateTime.class);
    }

    /**
     * Convert this attribute value into a {@link LocalTime} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a LocalTime.</li>
     * </ol>
     */
    default LocalTime asLocalTime() {
        return as(LocalTime.class);
    }

    /**
     * Convert this attribute value into a {@link Long} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a Long.</li>
     * </ol>
     */
    default Long asLong() {
        return as(Long.class);
    }

    /**
     * Convert this attribute value into a {@link MonthDay} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a MonthDay.</li>
     * </ol>
     */
    default MonthDay asMonthDay() {
        return as(MonthDay.class);
    }

    /**
     * Convert this attribute value into an {@link OffsetDateTime} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to an OffsetDateTime.</li>
     * </ol>
     */
    default OffsetDateTime asOffsetDateTime() {
        return as(OffsetDateTime.class);
    }

    /**
     * Convert this attribute value into a {@link Period} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a Period.</li>
     * </ol>
     */
    default Period asPeriod() {
        return as(Period.class);
    }

    /**
     * Convert this attribute value into a {@link String} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a String.</li>
     * </ol>
     */
    default String asString() {
        return as(String.class);
    }

    /**
     * Convert this attribute value into a {@link URI} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a URI.</li>
     * </ol>
     */
    default URI asUri() {
        return as(URI.class);
    }

    /**
     * Convert this attribute value into a {@link URL} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a URL.</li>
     * </ol>
     */
    default URL asUrl() {
        return as(URL.class);
    }

    /**
     * Convert this attribute value into a {@link UUID} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a UUID.</li>
     * </ol>
     */
    default UUID asUuid() {
        return as(UUID.class);
    }

    /**
     * Convert this attribute value into a {@link ZonedDateTime} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a ZonedDateTime.</li>
     * </ol>
     */
    default ZonedDateTime asZonedDateTime() {
        return as(ZonedDateTime.class);
    }

    /**
     * Convert this attribute value into a {@link ZoneId} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a ZoneId.</li>
     * </ol>
     */
    default ZoneId asZoneId() {
        return as(ZoneId.class);
    }

    /**
     * Convert this attribute value into a {@link ZoneOffset} using the {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>This attribute value cannot be converted to a ZoneOffset.</li>
     * </ol>
     */
    default ZoneOffset asZoneOffset() {
        return as(ZoneOffset.class);
    }

    /**
     * Convert this attribute value into an {@link Optional}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to an Optional of the requested type.</li>
     * </ol>
     */
    default <T> Optional<T> asOptionalOf(Class<T> optionalParameterType) {
        Validate.paramNotNull(optionalParameterType, "optionalParameterType");
        return as(TypeToken.optionalOf(optionalParameterType));
    }

    /**
     * Convert this attribute value into a {@link Collection}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Collection of the requested type.</li>
     * </ol>
     */
    default <T> Collection<T> asCollectionOf(Class<T> collectionParameterType) {
        Validate.paramNotNull(collectionParameterType, "collectionParameterType");
        return as(TypeToken.collectionOf(collectionParameterType));
    }

    /**
     * Convert this attribute value into a {@link Collection}, parameterized with the provided type token. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Collection of the requested type.</li>
     * </ol>
     */
    default <T> Collection<T> asCollectionOf(TypeToken<T> collectionParameterType) {
        Validate.paramNotNull(collectionParameterType, "collectionParameterType");
        return as(TypeToken.collectionOf(collectionParameterType));
    }

    /**
     * Convert this attribute value into a {@link List}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a List of the requested type.</li>
     * </ol>
     */
    default <T> List<T> asListOf(Class<T> listParameterType) {
        Validate.paramNotNull(listParameterType, "listParameterType");
        return as(TypeToken.listOf(listParameterType));
    }

    /**
     * Convert this attribute value into a {@link List}, parameterized with the provided type token. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a List of the requested type.</li>
     * </ol>
     */
    default <T> List<T> asListOf(TypeToken<T> listParameterType) {
        Validate.paramNotNull(listParameterType, "listParameterType");
        return as(TypeToken.listOf(listParameterType));
    }

    /**
     * Convert this attribute value into a {@link Set}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Set of the requested type.</li>
     * </ol>
     */
    default <T> Set<T> asSetOf(Class<T> setParameterType) {
        Validate.paramNotNull(setParameterType, "setParameterType");
        return as(TypeToken.setOf(setParameterType));
    }

    /**
     * Convert this attribute value into a {@link Set}, parameterized with the provided type token. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Set of the requested type.</li>
     * </ol>
     */
    default <T> Set<T> asSetOf(TypeToken<T> setParameterType) {
        Validate.paramNotNull(setParameterType, "setParameterType");
        return as(TypeToken.setOf(setParameterType));
    }

    /**
     * Convert this attribute value into a {@link Queue}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Queue of the requested type.</li>
     * </ol>
     */
    default <T> Queue<T> asQueueOf(Class<T> queueParameterType) {
        Validate.paramNotNull(queueParameterType, "queueParameterType");
        return as(TypeToken.queueOf(queueParameterType));
    }

    /**
     * Convert this attribute value into a {@link Queue}, parameterized with the provided type token. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Queue of the requested type.</li>
     * </ol>
     */
    default <T> Queue<T> asQueueOf(TypeToken<T> queueParameterType) {
        Validate.paramNotNull(queueParameterType, "queueParameterType");
        return as(TypeToken.queueOf(queueParameterType));
    }

    /**
     * Convert this attribute value into a {@link Deque}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Deque of the requested type.</li>
     * </ol>
     */
    default <T> Deque<T> asDequeOf(Class<T> dequeParameterType) {
        Validate.paramNotNull(dequeParameterType, "dequeParameterType");
        return as(TypeToken.dequeOf(dequeParameterType));
    }

    /**
     * Convert this attribute value into a {@link Deque}, parameterized with the provided type token. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a Deque of the requested type.</li>
     * </ol>
     */
    default <T> Deque<T> asDequeOf(TypeToken<T> dequeParameterType) {
        Validate.paramNotNull(dequeParameterType, "dequeParameterType");
        return as(TypeToken.dequeOf(dequeParameterType));
    }

    /**
     * Convert this attribute value into a {@link SortedSet}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a SortedSet of the requested type.</li>
     * </ol>
     */
    default <T> SortedSet<T> asSortedSetOf(Class<T> setParameterType) {
        Validate.paramNotNull(setParameterType, "setParameterType");
        return as(TypeToken.sortedSetOf(setParameterType));
    }

    /**
     * Convert this attribute value into a {@link SortedSet}, parameterized with the provided type token. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a SortedSet of the requested type.</li>
     * </ol>
     */
    default <T> SortedSet<T> asSortedSetOf(TypeToken<T> setParameterType) {
        Validate.paramNotNull(setParameterType, "setParameterType");
        return as(TypeToken.sortedSetOf(setParameterType));
    }

    /**
     * Convert this attribute value into a {@link NavigableSet}, parameterized with the provided class. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a NavigableSet of the requested type.</li>
     * </ol>
     */
    default <T> NavigableSet<T> asNavigableSetOf(Class<T> setParameterType) {
        Validate.paramNotNull(setParameterType, "setParameterType");
        return as(TypeToken.navigableSetOf(setParameterType));
    }

    /**
     * Convert this attribute value into a {@link NavigableSet}, parameterized with the provided type token. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided type is null.</li>
     *     <li>This attribute value cannot be converted to a NavigableSet of the requested type.</li>
     * </ol>
     */
    default <T> NavigableSet<T> asNavigableSetOf(TypeToken<T> setParameterType) {
        Validate.paramNotNull(setParameterType, "setParameterType");
        return as(TypeToken.navigableSetOf(setParameterType));
    }

    /**
     * Convert this attribute value into a {@link Map}, parameterized with the provided classes. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a Map of the requested types.</li>
     * </ol>
     */
    default <K, V> Map<K, V> asMapOf(Class<K> keyType, Class<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.mapOf(keyType, valueType));
    }

    /**
     * Convert this attribute value into a {@link Map}, parameterized with the provided type tokens. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a Map of the requested types.</li>
     * </ol>
     */
    default <K, V> Map<K, V> asMapOf(TypeToken<K> keyType, TypeToken<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.mapOf(keyType, valueType));
    }

    /**
     * Convert this attribute value into a {@link SortedMap}, parameterized with the provided classes. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a Map of the requested types.</li>
     * </ol>
     */
    default <K, V> SortedMap<K, V> asSortedMapOf(Class<K> keyType, Class<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.sortedMapOf(keyType, valueType));
    }

    /**
     * Convert this attribute value into a {@link SortedMap}, parameterized with the provided type tokens. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a Map of the requested types.</li>
     * </ol>
     */
    default <K, V> SortedMap<K, V> asSortedMapOf(TypeToken<K> keyType, TypeToken<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.sortedMapOf(keyType, valueType));
    }

    /**
     * Convert this attribute value into a {@link ConcurrentMap}, parameterized with the provided classes. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a ConcurrentMap of the requested types.</li>
     * </ol>
     */
    default <K, V> ConcurrentMap<K, V> asConcurrentMapOf(Class<K> keyType, Class<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.concurrentMapOf(keyType, valueType));
    }

    /**
     * Convert this attribute value into a {@link ConcurrentMap}, parameterized with the provided type tokens. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a ConcurrentMap of the requested types.</li>
     * </ol>
     */
    default <K, V> ConcurrentMap<K, V> asConcurrentMapOf(TypeToken<K> keyType, TypeToken<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.concurrentMapOf(keyType, valueType));
    }

    /**
     * Convert this attribute value into a {@link NavigableMap}, parameterized with the provided classes. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a NavigableMap of the requested types.</li>
     * </ol>
     */
    default <K, V> NavigableMap<K, V> asNavigableMapOf(Class<K> keyType, Class<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.navigableMapOf(keyType, valueType));
    }

    /**
     * Convert this attribute value into a {@link NavigableMap}, parameterized with the provided type tokens. This uses the
     * {@link AttributeConverter}s configured in the SDK.
     *
     * <p>
     * Reasons this call may fail with a {@link RuntimeException}:
     * <ol>
     *     <li>The provided key or value type is null.</li>
     *     <li>This attribute value cannot be converted to a NavigableMap of the requested types.</li>
     * </ol>
     */
    default <K, V> ConcurrentMap<K, V> asNavigableMapOf(TypeToken<K> keyType, TypeToken<V> valueType) {
        Validate.paramNotNull(keyType, "keyType");
        Validate.paramNotNull(valueType, "valueType");
        return as(TypeToken.concurrentMapOf(keyType, valueType));
    }
}
