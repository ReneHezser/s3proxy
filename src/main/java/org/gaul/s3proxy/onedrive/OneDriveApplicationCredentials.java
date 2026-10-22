/*
 * Copyright 2014-2026 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy.onedrive;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.function.Supplier;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredentialBuilder;

import org.gaul.s3proxy.blobstore.Credentials;
import org.jspecify.annotations.Nullable;

/** Keeps application tokens in memory and serializes renewal across requests. */
final class OneDriveApplicationCredentials implements Supplier<Credentials> {
    private static final Duration REFRESH_MARGIN = Duration.ofMinutes(1);

    private final TokenCredential credential;
    private final String drive;
    private final String scope;
    private final Clock clock;
    @Nullable
    private AccessToken cached;

    OneDriveApplicationCredentials(TokenCredential credential, String drive,
            String scope, Clock clock) {
        require(drive, "jclouds.identity (an explicit drive ID)");
        this.credential = credential;
        this.drive = drive;
        this.scope = scope;
        this.clock = clock;
    }

    static OneDriveApplicationCredentials configure(Supplier<Credentials> original,
            String endpoint, Properties properties) {
        Credentials configured = original.get();
        if (configured.sessionToken() != null &&
                !configured.sessionToken().isBlank()) {
            throw new IllegalArgumentException(
                    "OneDrive static access tokens are not supported; " +
                            "remove jclouds.session-token and configure " +
                            "application credentials");
        }
        String tenant = properties.getProperty(
                OneDriveBlobStore.PROPERTY_TENANT_ID, "");
        String client = properties.getProperty(
                OneDriveBlobStore.PROPERTY_CLIENT_ID, "");
        String secret = properties.getProperty(
                OneDriveBlobStore.PROPERTY_CLIENT_SECRET, "");
        String authority = properties.getProperty(
                OneDriveBlobStore.PROPERTY_AUTHORITY_HOST, "");
        require(tenant, OneDriveBlobStore.PROPERTY_TENANT_ID);
        require(client, OneDriveBlobStore.PROPERTY_CLIENT_ID);
        require(secret, OneDriveBlobStore.PROPERTY_CLIENT_SECRET);
        String drive = configured.identity();
        require(drive, "jclouds.identity (an explicit drive ID)");
        URI graph = URI.create(endpoint.isEmpty() ?
                "https://graph.microsoft.com/v1.0" : endpoint);
        String scope = graph.getScheme() + "://" + graph.getAuthority() +
                "/.default";
        URI authorityUri = URI.create(authority.isEmpty() ?
                "https://login.microsoftonline.com" : authority);
        if (!"https".equals(authorityUri.getScheme()) ||
                authorityUri.getHost() == null ||
                authorityUri.getUserInfo() != null ||
                authorityUri.getQuery() != null ||
                authorityUri.getFragment() != null ||
                !(authorityUri.getPath().isEmpty() ||
                        "/".equals(authorityUri.getPath()))) {
            throw new IllegalArgumentException(
                    "OneDrive authority-host must be an HTTPS origin");
        }
        var credential = new ClientSecretCredentialBuilder()
                .tenantId(tenant).clientId(client).clientSecret(secret)
                .authorityHost(authorityUri.toString()).build();
        return new OneDriveApplicationCredentials(credential, drive, scope,
                Clock.systemUTC());
    }

    private static void require(@Nullable String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "OneDrive application authentication requires " + property);
        }
    }

    @Override
    public synchronized Credentials get() {
        Instant now = clock.instant();
        AccessToken token = cached;
        if (token == null || !token.getExpiresAt().toInstant()
                .isAfter(now.plus(REFRESH_MARGIN))) {
            token = credential.getTokenSync(
                    new TokenRequestContext().addScopes(scope));
            if (token == null || token.getToken().isBlank() ||
                    !token.getExpiresAt().toInstant().isAfter(clock.instant())) {
                throw new IllegalStateException(
                        "OneDrive application authentication returned " +
                                "a missing or expired access token");
            }
            cached = token;
        }
        return new Credentials(drive, "", token.getToken());
    }
}
