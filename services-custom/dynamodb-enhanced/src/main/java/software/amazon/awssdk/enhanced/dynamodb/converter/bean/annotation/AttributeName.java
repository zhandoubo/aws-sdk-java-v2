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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * An annotation applied to a Java bean getter or setter that specifies the DynamoDB attribute name that should be used.
 *
 * <p>
 * If this is not specified, the value after "get" and "set" is de-capitalized and used. For example, "getUserId" -> "userId" or
 * "getURI" -> "uRI".
 *
 * <p>
 * See {@link AnnotatedBeanAttributeConverter} for usage examples.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target( {METHOD, FIELD })
@SdkPublicApi
public @interface AttributeName {
    String value();
}
