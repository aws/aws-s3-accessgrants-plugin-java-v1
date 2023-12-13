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

package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3control.model.Permission;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class CacheKey {
    final AWSCredentials credentials;
    final Permission permission;
    final String s3Prefix;

    private CacheKey(@NotNull AWSCredentials credentials, @NotNull Permission permission, @NotNull String s3Prefix) {

        this.credentials = credentials;
        this.permission = permission;
        this.s3Prefix = s3Prefix;
    }

    public CacheKey.Builder toBuilder() {
        return new CacheKey.BuilderImpl(this);
    }

    public static CacheKey.Builder builder() {
        return new CacheKey.BuilderImpl();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(credentials.getAWSAccessKeyId(), cacheKey.credentials.getAWSAccessKeyId()) &&
                Objects.equals(credentials.getAWSSecretKey(), cacheKey.credentials.getAWSSecretKey()) &&
                Objects.equals(s3Prefix, cacheKey.s3Prefix) &&
                Objects.equals(permission, cacheKey.permission);
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey(), s3Prefix, permission);
    }

    public interface Builder {
        CacheKey build();

        CacheKey.Builder credentials(AWSCredentials credentials);

        CacheKey.Builder permission(Permission permission);

        CacheKey.Builder s3Prefix(String s3Prefix);

    }

    static final class BuilderImpl implements CacheKey.Builder {
        private AWSCredentials credentials;
        private Permission permission;
        private String s3Prefix;

        private BuilderImpl() {
        }

        public BuilderImpl(CacheKey CacheKey) {
            credentials(CacheKey.credentials);
            permission(CacheKey.permission);
            s3Prefix(CacheKey.s3Prefix);
        }

        @Override
        public CacheKey build() {
            return new CacheKey(credentials, permission, s3Prefix);
        }

        @Override
        public Builder credentials(AWSCredentials credentials) {
            this.credentials = credentials;
            return this;
        }

        @Override
        public Builder permission(Permission permission) {
            this.permission = permission;
            return this;
        }

        @Override
        public Builder s3Prefix(String s3Prefix) {
            this.s3Prefix = s3Prefix;
            return this;
        }
    }


}
