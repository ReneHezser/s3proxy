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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.gaul.s3proxy.BlobStores;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.auth.AuthenticationType;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.Credentials;
import org.gaul.s3proxy.blobstore.MD5;
import org.gaul.s3proxy.blobstore.SdkRequests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

/** A local Graph fixture exercises requests, redirects and durable state. */
public final class OneDriveBlobStoreTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TIME = "2026-01-01T00:00:00Z";

    private final Map<String, JsonNode> nodes = new LinkedHashMap<>();
    private final Map<String, byte[]> data = new HashMap<>();
    private final List<String> authorization = new ArrayList<>();
    private final List<String> fragments = new ArrayList<>();
    private final List<String> requestPaths = new ArrayList<>();
    private final ByteArrayOutputStream session = new ByteArrayOutputStream();
    private final AtomicReference<Credentials> credentials =
            new AtomicReference<>(new Credentials("", "", "first-token"));
    private HttpServer server;
    private BlobStore store;
    private String base;
    private String uploadPath;
    private String nextLink;
    private int failure;
    private int uploadFailure;
    private boolean uploadCancelled;
    private int sequence;

    @BeforeEach
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(
                InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
        node("", true, new byte[0]);
        var properties = new Properties();
        properties.setProperty("jclouds.endpoint", base + "/v1.0");
        store = BlobStores.create("onedrive", properties, credentials::get);
    }

    @AfterEach
    public void tearDown() {
        store.close();
        server.stop(0);
    }

    @Test
    public void testBucketsAndObjectRoundTrip() throws IOException {
        store.createContainer("bucket");
        assertThat(store.list().buckets()).extracting("name")
                .containsExactly("bucket");
        assertThatThrownBy(() -> store.createContainer("bucket"))
                .isInstanceOf(S3Exception.class)
                .hasMessageContaining("already owned");

        String key = "folder/sp ace+#\u00e4.txt";
        byte[] content = "hello OneDrive".getBytes(StandardCharsets.UTF_8);
        var put = put(key, content);
        assertThat(put.eTag()).endsWith("-1");
        assertThat(store.blobMetadata("bucket", key).eTag())
                .isEqualTo(put.eTag());
        try (var get = store.getBlob("bucket", key)) {
            assertThat(get.readAllBytes()).isEqualTo(content);
        }
        try (var range = store.getBlob(GetObjectRequest.builder()
                .bucket("bucket").key(key).range("bytes=1-4").build())) {
            assertThat(range.readAllBytes()).isEqualTo(
                    Arrays.copyOfRange(content, 1, 5));
            assertThat(range.response().contentRange()).isEqualTo(
                    "bytes 1-4/" + content.length);
        }
        assertThatThrownBy(() -> store.deleteBucket("bucket"))
                .isInstanceOf(S3Exception.class);
        store.copyBlob(CopyObjectRequest.builder()
                .sourceBucket("bucket").sourceKey(key)
                .destinationBucket("bucket").destinationKey("copied")
                .build());
        try (var copied = store.getBlob("bucket", "copied")) {
            assertThat(copied.readAllBytes()).isEqualTo(content);
        }
        store.removeBlob("bucket", key);
        store.removeBlob("bucket", key);
        store.removeBlob("bucket", "copied");
        store.deleteBucket("bucket");
        assertThat(store.containerExists("bucket")).isFalse();
    }

    @Test
    public void testListingPagesPrefixesAndReservedFiles() {
        store.createContainer("bucket");
        put("a/file", new byte[] {1});
        put("b", new byte[] {2});
        put("c", new byte[] {3});
        var mpu = store.initiateMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket("bucket").key("pending").build());

        var request = ListObjectsV2Request.builder()
                .bucket("bucket").delimiter("/").maxKeys(2).build();
        var first = store.list(request);
        assertThat(first.commonPrefixes()).extracting("prefix")
                .containsExactly("a/");
        assertThat(first.contents()).extracting("key").containsExactly("b");
        assertThat(first.isTruncated()).isTrue();
        assertThat(store.list(request.toBuilder()
                .continuationToken(first.nextContinuationToken()).build())
                .contents()).extracting("key").containsExactly("c");
        assertThat(store.list(request.toBuilder().maxKeys(0).build())
                .keyCount()).isZero();
        assertThat(store.list(request.toBuilder().prefix("a/").build())
                .contents()).extracting("key").containsExactly("a/file");
        assertThat(store.listMultipartUploads("bucket")).extracting("key")
                .containsExactly("pending");
        store.abortMultipartUpload(mpu);
        assertThat(store.listMultipartUploads("bucket")).isEmpty();
    }

    @Test
    public void testLargeUploadsUseUnauthenticatedAlignedFragments()
            throws IOException {
        store.createContainer("bucket");
        byte[] content = new byte[11 * 1024 * 1024 + 7];
        Arrays.fill(content, (byte) 42);
        put("large", content);
        assertThat(fragments).containsExactly(
                "bytes 0-10485759/" + content.length,
                "bytes 10485760-" + (content.length - 1) + "/" +
                        content.length);
        try (var get = store.getBlob("bucket", "large")) {
            assertThat(get.readAllBytes()).isEqualTo(content);
        }
    }

    @Test
    public void testSimpleUploadBoundaryAndSessionCancellation() {
        store.createContainer("bucket");
        put("simple", new byte[4 * 1024 * 1024]);
        assertThat(fragments).isEmpty();
        put("session", new byte[4 * 1024 * 1024 + 1]);
        assertThat(fragments).hasSize(1);
        uploadFailure = 503;
        assertThatThrownBy(() -> put("failed",
                new byte[4 * 1024 * 1024 + 1]))
                .isInstanceOfSatisfying(S3Exception.class,
                        exception -> assertThat(exception.statusCode())
                                .isEqualTo(503));
        assertThat(uploadCancelled).isTrue();
        assertThat(store.blobExists("bucket", "failed")).isFalse();
    }

    @Test
    public void testExplicitDriveAndRootFolder() {
        JsonNode workspace = node("workspace", true, new byte[0]);
        credentials.set(new Credentials("drive-id", "", "drive-token"));
        var properties = new Properties();
        properties.setProperty("jclouds.endpoint", base + "/v1.0");
        properties.setProperty(OneDriveBlobStore.PROPERTY_ROOT,
                workspace.path("id").asText());
        store.close();
        store = BlobStores.create("onedrive", properties, credentials::get);
        store.createContainer("bucket");
        put("object", new byte[] {1});
        assertThat(store.list().buckets()).extracting("name")
                .containsExactly("bucket");
        assertThat(nodes).containsKey("workspace/bucket/object")
                .doesNotContainKey("bucket");
        assertThat(requestPaths).allMatch(path ->
                path.startsWith("/v1.0/drives/drive-id/"));
    }

    @Test
    public void testMultipartSurvivesStoreRecreation() throws IOException {
        store.createContainer("bucket");
        var mpu = store.initiateMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket("bucket").key("assembled").build());
        byte[] first = new byte[5 * 1024 * 1024];
        Arrays.fill(first, (byte) 7);
        byte[] last = {8, 9};
        var firstPart = store.uploadMultipartPart(mpu,
                UploadPartRequest.builder().partNumber(1)
                        .contentLength((long) first.length).build(),
                new ByteArrayInputStream(first));
        var lastPart = store.uploadMultipartPart(mpu,
                UploadPartRequest.builder().partNumber(2)
                        .contentLength((long) last.length).build(),
                new ByteArrayInputStream(last));
        store.close();
        store = new OneDriveBlobStore(credentials::get, base + "/v1.0", "");
        assertThat(store.listMultipartUpload(mpu)).extracting("partNumber")
                .containsExactly(1, 2);
        var completed = List.of(
                CompletedPart.builder().partNumber(1)
                        .eTag(firstPart.eTag()).build(),
                CompletedPart.builder().partNumber(2)
                        .eTag(lastPart.eTag()).build());
        store.completeMultipartUpload(mpu,
                SdkRequests.completeRequest(mpu, completed));
        assertThat(store.listMultipartUploads("bucket")).isEmpty();
        try (var result = store.getBlob("bucket", "assembled")) {
            byte[] actual = result.readAllBytes();
            assertThat(actual).hasSize(first.length + last.length);
            assertThat(Arrays.copyOfRange(actual, 0, first.length))
                    .isEqualTo(first);
            assertThat(Arrays.copyOfRange(actual, first.length, actual.length))
                    .isEqualTo(last);
        }
    }

    @Test
    public void testCredentialsRefreshAndErrorsAreNotHidden() {
        store.list();
        credentials.set(new Credentials("", "", "second-token"));
        store.list();
        assertThat(authorization).containsExactly(
                "Bearer first-token", "Bearer second-token");
        failure = 403;
        assertThatThrownBy(() -> store.containerExists("bucket"))
                .isInstanceOfSatisfying(S3Exception.class,
                        exception -> assertThat(exception.statusCode())
                                .isEqualTo(403));
        failure = 429;
        assertThatThrownBy(() -> store.list())
                .isInstanceOfSatisfying(S3Exception.class,
                        exception -> assertThat(exception.statusCode())
                                .isEqualTo(429));
        failure = 400;
        assertThatThrownBy(() -> store.list())
                .isInstanceOf(S3Exception.class)
                .hasMessageContaining("Graph rejected");
    }

    @Test
    public void testRejectsNamesChecksumsAndUnsupportedFeatures() {
        store.createContainer("bucket");
        for (String key : List.of("../escape", "a//b", "a/", "a\\b",
                ".S3PROXY-MULTIPART/hidden", "./file", "a:b", "trailing.")) {
            assertThatThrownBy(() -> put(key, new byte[0]))
                    .isInstanceOf(S3Exception.class);
        }
        byte[] payload = {1, 2, 3};
        assertThatThrownBy(() -> store.putBlob(PutObjectRequest.builder()
                .bucket("bucket").key("bad-length").contentLength(4L).build(),
                new ByteArrayInputStream(payload)))
                .isInstanceOf(S3Exception.class);
        assertThatThrownBy(() -> store.putBlob(PutObjectRequest.builder()
                .bucket("bucket").key("bad-md5").contentLength(3L)
                .contentMD5(Base64.getEncoder().encodeToString(
                        MD5.hash(new byte[0]))).build(),
                new ByteArrayInputStream(payload)))
                .isInstanceOf(S3Exception.class);
        assertThat(store.list("bucket").contents()).isEmpty();
        assertThatThrownBy(() -> store.putBlob(PutObjectRequest.builder()
                .bucket("bucket").key("conditional").ifNoneMatch("*")
                .build(), new ByteArrayInputStream(payload)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> store.setContainerAccess("bucket",
                software.amazon.awssdk.services.s3.model.BucketCannedACL
                        .PUBLIC_READ))
                .isInstanceOf(UnsupportedOperationException.class);
        put("file", payload);
        assertThatThrownBy(() -> store.setBlobAccess("bucket", "file",
                ObjectCannedACL.PUBLIC_READ))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testRejectsUntrustedPaginationLinkAndMissingToken() {
        nextLink = "https://example.invalid/steal-token";
        assertThatThrownBy(() -> store.list())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the configured endpoint");
        assertThatThrownBy(() -> new OneDriveBlobStore(
                () -> new Credentials("", "", null), base + "/v1.0", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("session-token");
    }

    @Test
    public void testS3RequestsReachOneDrive() throws Exception {
        var proxy = S3Proxy.builder().stopTimeout(0).blobStore(store)
                .awsAuthentication(AuthenticationType.AWS_V4,
                        "identity", "credential")
                .endpoint(URI.create("http://127.0.0.1:0")).build();
        proxy.start();
        try (var s3 = S3Client.builder()
                .endpointOverride(URI.create("http://127.0.0.1:" +
                        proxy.getPort()))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("identity", "credential")))
                .build()) {
            s3.createBucket(request -> request.bucket("bucket"));
            var put = s3.putObject(request -> request.bucket("bucket")
                            .key("object"),
                    RequestBody.fromString("through S3"));
            assertThat(put.eTag()).contains("-1");
            assertThat(s3.getObjectAsBytes(request -> request.bucket("bucket")
                    .key("object")).asUtf8String()).isEqualTo("through S3");
            assertThat(s3.listObjectsV2(request -> request.bucket("bucket"))
                    .contents()).extracting("key").containsExactly("object");
            assertThatThrownBy(() -> s3.putObject(request ->
                            request.bucket("bucket").key("conditional")
                                    .ifNoneMatch("*"),
                    RequestBody.fromString("not written")))
                    .isInstanceOfSatisfying(S3Exception.class,
                            exception -> assertThat(exception.statusCode())
                                    .isEqualTo(501));
            var upload = s3.createMultipartUpload(request ->
                    request.bucket("bucket").key("multipart"));
            assertThatThrownBy(() -> s3.listParts(request ->
                            request.bucket("bucket").key("wrong-key")
                                    .uploadId(upload.uploadId())))
                    .isInstanceOfSatisfying(S3Exception.class,
                            exception -> assertThat(
                                    exception.awsErrorDetails().errorCode())
                                    .isEqualTo("NoSuchUpload"));
            var part = s3.uploadPart(request -> request.bucket("bucket")
                            .key("multipart").uploadId(upload.uploadId())
                            .partNumber(1),
                    RequestBody.fromString("multipart through S3"));
            s3.completeMultipartUpload(request -> request.bucket("bucket")
                    .key("multipart").uploadId(upload.uploadId())
                    .multipartUpload(completed -> completed.parts(
                            CompletedPart.builder().partNumber(1)
                                    .eTag(part.eTag()).build())));
            assertThat(s3.getObjectAsBytes(request -> request.bucket("bucket")
                    .key("multipart")).asUtf8String())
                    .isEqualTo("multipart through S3");
        } finally {
            proxy.stop();
        }
    }

    private software.amazon.awssdk.services.s3.model.PutObjectResponse put(
            String key, byte[] payload) {
        return store.putBlob(PutObjectRequest.builder()
                .bucket("bucket").key(key)
                .contentLength((long) payload.length).build(),
                new ByteArrayInputStream(payload));
    }

    private JsonNode node(String path, boolean folder, byte[] content) {
        var result = MAPPER.createObjectNode();
        result.put("id", "item-" + sequence++);
        result.put("name", path.substring(path.lastIndexOf('/') + 1));
        result.put("size", content.length);
        result.put("eTag", "\"" + sequence + "\"");
        result.put("createdDateTime", TIME);
        result.put("lastModifiedDateTime", TIME);
        result.set(folder ? "folder" : "file", MAPPER.createObjectNode());
        nodes.put(path.toLowerCase(Locale.ROOT), result);
        data.put(result.path("id").asText(), content);
        return result;
    }

    private String pathById(String id) {
        return nodes.entrySet().stream()
                .filter(entry -> id.equals(entry.getValue().path("id")
                        .asText()))
                .map(Map.Entry::getKey).findFirst().orElse("missing");
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            String auth = exchange.getRequestHeaders()
                    .getFirst("Authorization");
            if (path.startsWith("/download/")) {
                assertThat(auth).isNull();
                byte[] content = data.get(path.substring(
                        "/download/".length()));
                String range = exchange.getRequestHeaders().getFirst("Range");
                if (range != null) {
                    String[] ends = range.substring(6).split("-", -1);
                    int start = Integer.parseInt(ends[0]);
                    int end = Integer.parseInt(ends[1]);
                    exchange.getResponseHeaders().add("Content-Range",
                            "bytes " + start + "-" + end + "/" +
                                    content.length);
                    respond(exchange, 206,
                            Arrays.copyOfRange(content, start, end + 1));
                } else {
                    respond(exchange, 200, content);
                }
                return;
            }
            if ("/upload".equals(path)) {
                assertThat(auth).isNull();
                if ("DELETE".equals(method)) {
                    uploadCancelled = true;
                    respond(exchange, 204, new byte[0]);
                    return;
                }
                if (uploadFailure != 0) {
                    respondJson(exchange, uploadFailure, Map.of());
                    return;
                }
                String range = exchange.getRequestHeaders().getFirst(
                        "Content-Range");
                fragments.add(range);
                session.write(exchange.getRequestBody().readAllBytes());
                long total = Long.parseLong(range.substring(
                        range.indexOf('/') + 1));
                if (session.size() == total) {
                    respondJson(exchange, 201,
                            node(uploadPath, false, session.toByteArray()));
                } else {
                    respondJson(exchange, 202, Map.of("nextExpectedRanges",
                            List.of(session.size() + "-")));
                }
                return;
            }
            authorization.add(auth);
            requestPaths.add(path);
            if (failure != 0) {
                respondJson(exchange, failure, Map.of());
                return;
            }
            String relative = path.replaceFirst(
                    "^/v1.0/(me/drive|drives/[^/]+)/", "");
            assertThat(relative).doesNotEndWith(":");
            int colon = relative.indexOf(":/");
            String suffix = "";
            if (colon != -1) {
                int end = relative.indexOf(':', colon + 2);
                suffix = relative.substring(colon + 2,
                        end == -1 ? relative.length() : end);
                relative = relative.substring(0, colon) +
                        (end == -1 ? "" : relative.substring(end + 1));
            }
            String operation = "";
            for (String candidate : List.of("/children", "/content",
                    "/createUploadSession")) {
                if (relative.endsWith(candidate)) {
                    operation = candidate;
                    relative = relative.substring(0,
                            relative.length() - candidate.length());
                    break;
                }
            }
            String parent = "root".equals(relative) ? "" :
                    pathById(relative.substring("items/".length()));
            String target = parent + (parent.isEmpty() || suffix.isEmpty() ?
                    "" : "/") + suffix.toLowerCase(Locale.ROOT);
            JsonNode existing = nodes.get(target);
            if ("/content".equals(operation) && "PUT".equals(method)) {
                respondJson(exchange, 201, node(target, false,
                        exchange.getRequestBody().readAllBytes()));
            } else if ("/createUploadSession".equals(operation)) {
                uploadPath = target;
                session.reset();
                respondJson(exchange, 200, Map.of("uploadUrl", base +
                        "/upload"));
            } else if (existing == null) {
                respondJson(exchange, 404, Map.of());
            } else if ("/children".equals(operation) &&
                    "POST".equals(method)) {
                String name = MAPPER.readTree(exchange.getRequestBody())
                        .path("name").asText();
                String child = target + (target.isEmpty() ? "" : "/") + name;
                if (nodes.containsKey(child.toLowerCase(Locale.ROOT))) {
                    respondJson(exchange, 409, Map.of());
                } else {
                    respondJson(exchange, 201,
                            node(child, true, new byte[0]));
                }
            } else if ("/children".equals(operation)) {
                var children = nodes.entrySet().stream().filter(entry -> {
                    String key = entry.getKey();
                    return !key.isEmpty() && !key.equals(target) &&
                            key.substring(0, Math.max(0,
                                    key.lastIndexOf('/'))).equals(target);
                }).map(Map.Entry::getValue).toList();
                String query = exchange.getRequestURI().getQuery();
                int offset = query == null ? 0 :
                        Integer.parseInt(query.substring("offset=".length()));
                var page = MAPPER.createObjectNode();
                page.set("value", MAPPER.valueToTree(children.subList(offset,
                        Math.min(offset + 2, children.size()))));
                if (nextLink != null) {
                    page.put("@odata.nextLink", nextLink);
                } else if (offset + 2 < children.size()) {
                    page.put("@odata.nextLink", base +
                            exchange.getRequestURI().getRawPath() +
                            "?offset=" + (offset + 2));
                }
                respondJson(exchange, 200, page);
            } else if ("/content".equals(operation)) {
                exchange.getResponseHeaders().add("Location",
                        base + "/download/" + existing.path("id").asText());
                respond(exchange, 302, new byte[0]);
            } else if ("DELETE".equals(method)) {
                nodes.keySet().removeIf(key -> key.equals(target) ||
                        key.startsWith(target + "/"));
                respond(exchange, 204, new byte[0]);
            } else {
                respondJson(exchange, 200, existing);
            }
        }
    }

    private static void respondJson(HttpExchange exchange, int status,
            Object value) throws IOException {
        respond(exchange, status, MAPPER.writeValueAsBytes(value));
    }

    private static void respond(HttpExchange exchange, int status, byte[] body)
            throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ?
                -1 : body.length);
        exchange.getResponseBody().write(body);
    }
}
