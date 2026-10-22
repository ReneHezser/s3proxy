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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;

import org.gaul.s3proxy.BlobStores;
import org.gaul.s3proxy.blobstore.Credentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import reactor.core.publisher.Mono;

public final class OneDriveApplicationCredentialsTest {
    private static final String SCOPE = "https://graph.microsoft.com/.default";
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    public void testCachesAndRefreshesBeforeExpiry() {
        var clock = new MutableClock();
        var calls = new AtomicInteger();
        TokenCredential credential = context -> {
            assertThat(context.getScopes()).containsExactly(SCOPE);
            int count = calls.incrementAndGet();
            return Mono.just(new AccessToken("application-token-" + count,
                    OffsetDateTime.ofInstant(clock.instant().plusSeconds(3600),
                            ZoneOffset.UTC)));
        };
        var supplier = new OneDriveApplicationCredentials(credential,
                "drive-id", SCOPE, clock);
        assertThat(supplier.get()).isEqualTo(
                new Credentials("drive-id", "", "application-token-1"));
        clock.now = START.plusSeconds(3539);
        assertThat(supplier.get().sessionToken())
                .isEqualTo("application-token-1");
        assertThat(calls.get()).isEqualTo(1);
        clock.now = START.plusSeconds(3540);
        assertThat(supplier.get().sessionToken())
                .isEqualTo("application-token-2");
        assertThat(calls.get()).isEqualTo(2);
        clock.now = START.plusSeconds(8000);
        assertThat(supplier.get().sessionToken())
                .isEqualTo("application-token-3");
    }

    @Test
    public void testConcurrentRequestsShareRenewal() throws Exception {
        var clock = new MutableClock();
        var calls = new AtomicInteger();
        TokenCredential credential = context -> Mono.just(new AccessToken(
                "application-token-" + calls.incrementAndGet(),
                OffsetDateTime.ofInstant(clock.instant().plusSeconds(3600),
                        ZoneOffset.UTC)));
        var supplier = new OneDriveApplicationCredentials(credential,
                "drive-id", SCOPE, clock);
        var executor = Executors.newFixedThreadPool(8);
        try {
            for (int round = 1; round <= 2; ++round) {
                var tasks = Collections.<Callable<Credentials>>nCopies(24,
                        supplier::get);
                for (var result : executor.invokeAll(tasks)) {
                    assertThat(result.get().sessionToken())
                            .isEqualTo("application-token-" + round);
                }
                assertThat(calls.get()).isEqualTo(round);
                clock.now = START.plusSeconds(3540);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testRenewalFailureIsExplicitAndCanRecover() {
        var clock = new MutableClock();
        var calls = new AtomicInteger();
        TokenCredential credential = context -> {
            int count = calls.incrementAndGet();
            if (count == 2) {
                return Mono.error(new IllegalStateException(
                        "Identity provider unavailable"));
            }
            return Mono.just(new AccessToken("application-token-" + count,
                    OffsetDateTime.ofInstant(clock.instant().plusSeconds(3600),
                            ZoneOffset.UTC)));
        };
        var supplier = new OneDriveApplicationCredentials(credential,
                "drive-id", SCOPE, clock);
        supplier.get();
        clock.now = START.plusSeconds(3600);
        assertThatThrownBy(supplier::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Identity provider unavailable");
        assertThat(supplier.get().sessionToken())
                .isEqualTo("application-token-3");
    }

    @ParameterizedTest
    @ValueSource(strings = {"expired", "empty", "missing"})
    public void testRejectsInvalidTokenResponses(String response) {
        var clock = new MutableClock();
        TokenCredential credential = context -> switch (response) {
        case "expired" -> Mono.just(new AccessToken("expired-token",
                OffsetDateTime.ofInstant(START, ZoneOffset.UTC)));
        case "empty" -> Mono.just(new AccessToken("",
                OffsetDateTime.ofInstant(START.plusSeconds(3600),
                        ZoneOffset.UTC)));
        case "missing" -> Mono.empty();
        default -> throw new IllegalArgumentException(response);
        };
        var supplier = new OneDriveApplicationCredentials(credential,
                "drive-id", SCOPE, clock);
        assertThatThrownBy(supplier::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing or expired");
    }

    @Test
    public void testRejectsMissingApplicationConfiguration() {
        assertThatThrownBy(() -> BlobStores.create("onedrive",
                new Properties()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires s3proxy.onedrive.tenant-id");
    }

    @Test
    public void testRejectsStaticAccessTokenWithoutApplicationConfiguration() {
        var properties = new Properties();
        properties.setProperty("jclouds.identity", "drive-id");
        properties.setProperty("jclouds.session-token", "static-token");
        assertThatThrownBy(() -> BlobStores.create("onedrive", properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("static access tokens are not supported");
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3proxy.onedrive.tenant-id",
        "s3proxy.onedrive.client-id", "s3proxy.onedrive.client-secret"})
    public void testRejectsIncompleteApplicationConfiguration(String missing) {
        var properties = applicationProperties();
        properties.remove(missing);
        assertThatThrownBy(() -> BlobStores.create("onedrive", properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(missing);
    }

    @Test
    public void testRequiresExplicitDrive() {
        var properties = applicationProperties();
        properties.setProperty("jclouds.identity", "");
        assertThatThrownBy(() -> BlobStores.create("onedrive", properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("explicit drive ID");
    }

    @Test
    public void testRejectsMixedAuthenticationModes() {
        var properties = applicationProperties();
        properties.setProperty("jclouds.session-token", "static-token");
        assertThatThrownBy(() -> BlobStores.create("onedrive", properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("static access tokens are not supported");
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://login.microsoftonline.com",
        "https://user:password@login.microsoftonline.com",
        "https://login.microsoftonline.com/path",
        "https://login.microsoftonline.com?query=1",
        "https://login.microsoftonline.com#fragment"})
    public void testRejectsInvalidAuthorityHosts(String authority) {
        var properties = applicationProperties();
        properties.setProperty(OneDriveBlobStore.PROPERTY_AUTHORITY_HOST,
                authority);
        assertThatThrownBy(() -> BlobStores.create("onedrive", properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HTTPS origin");
    }

    @Test
    public void testBuildsApplicationProviderWithoutStaticToken() {
        var properties = applicationProperties();
        assertThat(OneDriveApplicationCredentials.configure(
                () -> new Credentials("drive-id", ""), "", properties))
                .isInstanceOf(OneDriveApplicationCredentials.class);
    }

    @Test
    public void testCommandLineAcceptsApplicationCredentialsWithoutLegacyPair()
            throws Exception {
        var properties = applicationProperties();
        properties.setProperty("jclouds.provider", "onedrive");
        properties.remove(OneDriveBlobStore.PROPERTY_TENANT_ID);
        Path temporary = Files.createTempFile("onedrive-application-", ".conf");
        try {
            try (var output = Files.newOutputStream(temporary)) {
                properties.store(output, null);
            }
            var process = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java")
                            .toString(),
                    "-cp", System.getProperty("surefire.test.class.path",
                            System.getProperty("java.class.path")),
                    "org.gaul.s3proxy.Main", "--properties",
                    temporary.toString())
                    .redirectErrorStream(true).start();
            try {
                assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
                String output = new String(
                        process.getInputStream().readAllBytes(),
                        StandardCharsets.UTF_8);
                assertThat(process.exitValue()).isNotZero();
                assertThat(output)
                        .contains("requires s3proxy.onedrive.tenant-id")
                        .doesNotContain("Properties file must contain");
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static Properties applicationProperties() {
        var properties = new Properties();
        properties.setProperty("jclouds.identity", "drive-id");
        properties.setProperty(OneDriveBlobStore.PROPERTY_TENANT_ID,
                "00000000-0000-0000-0000-000000000001");
        properties.setProperty(OneDriveBlobStore.PROPERTY_CLIENT_ID,
                "00000000-0000-0000-0000-000000000002");
        properties.setProperty(OneDriveBlobStore.PROPERTY_CLIENT_SECRET,
                "fixture-client-secret");
        return properties;
    }

    private static final class MutableClock extends Clock {
        private Instant now = START;

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(now, zone);
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
