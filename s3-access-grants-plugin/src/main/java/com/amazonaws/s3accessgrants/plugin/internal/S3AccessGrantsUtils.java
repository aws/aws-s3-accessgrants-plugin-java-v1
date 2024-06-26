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

package com.amazonaws.s3accessgrants.plugin.internal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.amazonaws.util.ValidationUtils;

public class S3AccessGrantsUtils {
    public static final Boolean DEFAULT_FALLBACK = true;
    public static final Boolean DEFAULT_CROSS_REGION_ACCESS = false;
    public static final int DEFAULT_DURATION = 3600;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsUtils.class);

    public static void argumentNotNull(Object param, String message) {
        try {
            ValidationUtils.assertNotNull(param, message);
        } catch (NullPointerException e) {
            logger.error(message);
            throw new IllegalArgumentException(message);
        }
    }
}
