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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;

import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.Credentials;
import org.gaul.s3proxy.blobstore.MD5;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.gaul.s3proxy.blobstore.SdkResponses;
import org.gaul.s3proxy.blobstore.domain.MultipartUpload;
import org.jspecify.annotations.Nullable;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Microsoft Graph drive storage. Buckets are folders and keys are paths
 * relative to them; Graph's case-insensitive namespace remains in effect.
 */
public final class OneDriveBlobStore implements BlobStore {
    public static final String PROPERTY_ROOT = "s3proxy.onedrive.root";

    private static final String MULTIPART = ".s3proxy-multipart";
    private static final int SIMPLE_UPLOAD_LIMIT = 4 * 1024 * 1024;
    private static final int MAX_DOWNLOAD_ATTEMPTS = 3;
    // Graph requires nonfinal fragments to be multiples of 320 KiB.
    private static final int FRAGMENT_SIZE = 32 * 320 * 1024;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Supplier<Credentials> creds;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    private final URI endpoint;
    private final String drive;
    private final String root;

    public OneDriveBlobStore(Supplier<Credentials> creds, String endpoint,
            String root) {
        this.creds = creds;
        this.endpoint = URI.create(endpoint.isEmpty() ?
                "https://graph.microsoft.com/v1.0" :
                endpoint.replaceAll("/+$", ""));
        validateUrl(this.endpoint);
        if (this.endpoint.getQuery() != null) {
            throw new IllegalArgumentException(
                    "OneDrive Graph endpoint must not contain a query");
        }
        String identity = creds.get().identity();
        this.drive = identity == null || identity.isEmpty() ?
                "/me/drive" : "/drives/" + encode(identity);
        this.root = root.isEmpty() || "root".equals(root) ?
                "/root" : "/items/" + encode(root);
        token();
    }
    @Override
    public HeadBucketResponse headBucket(HeadBucketRequest request) {
        bucket(request.bucket());
        return HeadBucketResponse.builder().build();
    }

    @Override
    public CreateBucketResponse createContainer(CreateBucketRequest request) {
        validateSegment(request.bucket());
        privateAccess(request.acl() == null ||
                request.acl() == BucketCannedACL.PRIVATE);
        try {
            createFolder(item(""), request.bucket());
        } catch (S3Exception se) {
            if (se.statusCode() == 409) {
                throw S3Exceptions.bucketAlreadyOwnedByYou(request.bucket());
            }
            throw se;
        }
        return CreateBucketResponse.builder().build();
    }

    @Override
    public BucketCannedACL getContainerAccess(String container) {
        bucket(container);
        return BucketCannedACL.PRIVATE;
    }

    @Override
    public void setContainerAccess(String container, BucketCannedACL access) {
        bucket(container);
        privateAccess(access == BucketCannedACL.PRIVATE);
    }

    @Override
    public void deleteBucket(String container) {
        if (!containerExists(container)) {
            return;
        }
        if (!list(container).contents().isEmpty() ||
                !listMultipartUploads(container).isEmpty()) {
            throw S3Exceptions.bucketNotEmpty(container);
        }
        // Graph deletes empty subdirectories left by object deletes too.
        json("DELETE", item(container), null);
    }

    @Override
    public ListBucketsResponse list() {
        var buckets = children(item("")).stream()
                .filter(node -> node.has("folder"))
                .map(node -> SdkResponses.bucket(node.path("name").asText(),
                        created(node)))
                .toList();
        return ListBucketsResponse.builder().buckets(buckets).build();
    }

    @Override
    public ListObjectsV2Response list(ListObjectsV2Request request) {
        bucket(request.bucket());
        var entries = new TreeMap<String, JsonNode>();
        collect(item(request.bucket()), "", entries);
        String prefix = request.prefix() == null ? "" : request.prefix();
        String marker = request.continuationToken() == null ?
                request.startAfter() : request.continuationToken();
        String delimiter = request.delimiter();
        var selected = new TreeMap<String, JsonNode>();
        for (var entry : entries.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(prefix)) {
                continue;
            }
            boolean commonPrefix = false;
            if (delimiter != null && !delimiter.isEmpty()) {
                int index = key.indexOf(delimiter, prefix.length());
                if (index != -1) {
                    key = key.substring(0, index + delimiter.length());
                    commonPrefix = true;
                }
            }
            if (marker == null || key.compareTo(marker) > 0) {
                selected.put(key, commonPrefix ? MAPPER.createObjectNode() :
                        entry.getValue());
            }
        }
        int maxKeys = request.maxKeys() == null ? 1000 : request.maxKeys();
        var objects = new ArrayList<S3Object>();
        var prefixes = new ArrayList<CommonPrefix>();
        String last = null;
        int count = 0;
        for (var entry : selected.entrySet()) {
            if (count >= maxKeys) {
                break;
            }
            last = entry.getKey();
            if (entry.getValue().isEmpty()) {
                prefixes.add(SdkResponses.commonPrefix(last));
            } else {
                JsonNode node = entry.getValue();
                objects.add(SdkResponses.objectEntry(last, etag(node),
                        modified(node), node.path("size").asLong(),
                        StorageClass.STANDARD));
            }
            ++count;
        }
        boolean truncated = maxKeys > 0 && selected.size() > count;
        return ListObjectsV2Response.builder()
                .contents(objects).commonPrefixes(prefixes)
                .keyCount(count).maxKeys(maxKeys)
                .isTruncated(truncated)
                .nextContinuationToken(truncated ? last : null)
                .build();
    }

    private void collect(String folder, String prefix,
            Map<String, JsonNode> entries) {
        for (JsonNode node : children(folder)) {
            String name = node.path("name").asText();
            if (prefix.isEmpty() && MULTIPART.equalsIgnoreCase(name)) {
                continue;
            }
            String key = prefix + name;
            if (node.has("folder")) {
                collect(byId(node), key + "/", entries);
            } else if (node.has("file")) {
                entries.put(key, node);
            }
        }
    }

    @Override
    public PutObjectResponse putBlob(PutObjectRequest request,
            InputStream payload) {
        validateKey(request.key());
        privateAccess(request.acl() == null ||
                request.acl() == ObjectCannedACL.PRIVATE);
        rejectEncryption(request.serverSideEncryptionAsString(),
                request.sseCustomerAlgorithm());
        rejectConditions(request.ifMatch(), request.ifNoneMatch());
        bucket(request.bucket());
        JsonNode result = putFile(request.bucket() + "/" + request.key(),
                request.contentLength(), request.contentMD5(), payload);
        return PutObjectResponse.builder().eTag(etag(result)).build();
    }

    @Override
    public HeadObjectResponse blobMetadata(HeadObjectRequest request) {
        rejectVersion(request.versionId());
        JsonNode node = object(request.bucket(), request.key());
        return SdkResponses.toHead(metadata(node));
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getBlob(
            GetObjectRequest request) {
        rejectVersion(request.versionId());
        var headers = request.range() == null ? Map.<String, String>of() :
                Map.of("Range", request.range());
        for (int attempt = 0; attempt < MAX_DOWNLOAD_ATTEMPTS; ++attempt) {
            JsonNode node = object(request.bucket(), request.key());
            enforceConditionalGet(request, node);
            Path temporary = temporaryFile();
            boolean retained = false;
            try {
                var response = send("GET", byId(node) + "/content",
                        HttpRequest.BodyPublishers.noBody(), headers, true);
                if (response.statusCode() == 302 ||
                        response.statusCode() == 307) {
                    closeBody(response);
                    String location = response.headers().firstValue("Location")
                            .orElseThrow(() -> new IllegalStateException(
                                    "Graph download redirect has no Location"));
                    response = send("GET", location,
                            HttpRequest.BodyPublishers.noBody(), headers, false);
                }
                try (var input = response.body()) {
                    if (response.statusCode() >= 200 &&
                            response.statusCode() < 300) {
                        Files.copy(input, temporary,
                                StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                // The item ID survives overwrites. Verify the revision after
                // consuming the body, before exposing any bytes to the caller.
                JsonNode current = object(request.bucket(), request.key());
                if (!node.path("id").equals(current.path("id")) ||
                        !node.path("eTag").equals(current.path("eTag"))) {
                    continue;
                }
                check(response);
                var result = metadata(node).toBuilder()
                        .contentLength(Files.size(temporary))
                        .contentRange(response.headers().firstValue(
                                "Content-Range").orElse(null))
                        .build();
                var download = SdkResponses.getResponse(result,
                        Files.newInputStream(temporary,
                                StandardOpenOption.DELETE_ON_CLOSE));
                retained = true;
                return download;
            } catch (IOException ioe) {
                throw new UncheckedIOException(
                        "OneDrive download staging failed", ioe);
            } finally {
                if (!retained) {
                    deleteTemporary(temporary);
                }
            }
        }
        throw S3Exceptions.fromStatusCode(503,
                new IllegalStateException(
                        "OneDrive object changed during every download attempt"));
    }

    // Report the actual status: the frontend cannot infer which condition
    // failed when a request carries multiple conditional headers.
    private static void enforceConditionalGet(GetObjectRequest request,
            JsonNode node) {
        String tag = etag(node);
        String ifMatch = request.ifMatch();
        String ifNoneMatch = request.ifNoneMatch();
        if (ifMatch != null && !"*".equals(ifMatch) &&
                !tag.equals(unquote(ifMatch))) {
            throw conditionFailed(412, tag);
        }
        if (ifNoneMatch != null && ("*".equals(ifNoneMatch) ||
                tag.equals(unquote(ifNoneMatch)))) {
            throw conditionFailed(304, tag);
        }
        // HTTP dates have one-second granularity, Graph's do not.
        Instant modified = modified(node).truncatedTo(ChronoUnit.SECONDS);
        Instant modifiedSince = request.ifModifiedSince();
        if (ifNoneMatch == null && modifiedSince != null &&
                !modified.isAfter(modifiedSince)) {
            throw conditionFailed(304, tag);
        }
        Instant unmodifiedSince = request.ifUnmodifiedSince();
        if (ifMatch == null && unmodifiedSince != null &&
                modified.isAfter(unmodifiedSince)) {
            throw conditionFailed(412, tag);
        }
    }

    private static S3Exception conditionFailed(int status, String tag) {
        return S3Exceptions.fromStatusCode(status, "\"" + tag + "\"", Map.of(),
                /*cause=*/ null);
    }

    @Override
    public CopyObjectResponse copyBlob(CopyObjectRequest request) {
        rejectEncryption(request.serverSideEncryptionAsString(),
                request.sseCustomerAlgorithm());
        rejectVersion(request.sourceVersionId());
        if (request.copySourceIfMatch() != null ||
                request.copySourceIfNoneMatch() != null ||
                request.copySourceIfModifiedSince() != null ||
                request.copySourceIfUnmodifiedSince() != null) {
            throw new UnsupportedOperationException(
                    "OneDrive conditional copies are not supported");
        }
        var get = GetObjectRequest.builder()
                .bucket(request.sourceBucket())
                .key(request.sourceKey()).build();
        try (var input = getBlob(get)) {
            var put = putBlob(PutObjectRequest.builder()
                    .bucket(request.destinationBucket())
                    .key(request.destinationKey())
                    .acl(request.acl())
                    .contentLength(input.response().contentLength())
                    .build(), input);
            return CopyObjectResponse.builder()
                    .copyObjectResult(CopyObjectResult.builder()
                            .eTag(put.eTag())
                            .lastModified(blobMetadata(
                                    request.destinationBucket(),
                                    request.destinationKey()).lastModified())
                            .build())
                    .build();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public DeleteObjectResponse removeBlob(DeleteObjectRequest request) {
        rejectVersion(request.versionId());
        rejectConditions(request.ifMatch(), null);
        if (request.ifMatchSize() != null ||
                request.ifMatchLastModifiedTime() != null) {
            throw new UnsupportedOperationException(
                    "OneDrive conditional deletes are not supported");
        }
        validateKey(request.key());
        bucket(request.bucket());
        try {
            JsonNode node = object(request.bucket(), request.key());
            json("DELETE", byId(node), null);
        } catch (S3Exception se) {
            if (se.statusCode() != 404) {
                throw se;
            }
        }
        return DeleteObjectResponse.builder().build();
    }

    @Override
    public ObjectCannedACL getBlobAccess(String container, String name) {
        object(container, name);
        return ObjectCannedACL.PRIVATE;
    }

    @Override
    public void setBlobAccess(String container, String name,
            ObjectCannedACL access) {
        object(container, name);
        privateAccess(access == ObjectCannedACL.PRIVATE);
    }

    @Override
    public MultipartUpload initiateMultipartUpload(
            CreateMultipartUploadRequest request) {
        validateKey(request.key());
        privateAccess(request.acl() == null ||
                request.acl() == ObjectCannedACL.PRIVATE);
        rejectEncryption(request.serverSideEncryptionAsString(),
                request.sseCustomerAlgorithm());
        bucket(request.bucket());
        String id = UUID.randomUUID().toString();
        String path = request.bucket() + "/" + MULTIPART + "/" + id;
        ensureFolder(path);
        byte[] target = request.key().getBytes(StandardCharsets.UTF_8);
        try {
            putFile(path + "/target", (long) target.length, null,
                    new ByteArrayInputStream(target));
        } catch (RuntimeException failure) {
            try {
                json("DELETE", item(path), null);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        return new MultipartUpload(id, request);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        upload(mpu);
        json("DELETE", item(uploadPath(mpu)), null);
    }

    @Override
    public UploadPartResponse uploadMultipartPart(MultipartUpload mpu,
            UploadPartRequest request, InputStream input) {
        upload(mpu);
        if (request.partNumber() == null || request.partNumber() < 1 ||
                request.partNumber() > 10_000) {
            throw S3Exceptions.invalidArgument("Invalid part number");
        }
        var node = putFile(uploadPath(mpu) + "/" + request.partNumber(),
                request.contentLength(), request.contentMD5(), input);
        return SdkResponses.uploadedPart(etag(node));
    }

    @Override
    public List<Part> listMultipartUpload(MultipartUpload mpu) {
        upload(mpu);
        return children(item(uploadPath(mpu))).stream()
                .filter(node -> !"target".equals(node.path("name").asText()))
                .map(node -> SdkResponses.part(
                        Integer.parseInt(node.path("name").asText()),
                        node.path("size").asLong(), etag(node), modified(node)))
                .sorted(Comparator.comparing(Part::partNumber))
                .toList();
    }

    @Override
    public List<software.amazon.awssdk.services.s3.model.MultipartUpload>
            listMultipartUploads(String container) {
        bucket(container);
        JsonNode folder;
        try {
            folder = json("GET", item(container + "/" + MULTIPART), null);
        } catch (S3Exception se) {
            if (se.statusCode() == 404) {
                return List.of();
            }
            throw se;
        }
        var uploads = new ArrayList<
                software.amazon.awssdk.services.s3.model.MultipartUpload>();
        for (JsonNode node : children(byId(folder))) {
            try {
                JsonNode target = json("GET", byId(node) + ":/target:", null);
                uploads.add(SdkResponses.upload(readTarget(target),
                        node.path("name").asText(), created(node)));
            } catch (S3Exception se) {
                if (se.statusCode() != 404) {
                    throw se;
                }
            }
        }
        return uploads;
    }

    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(
            MultipartUpload mpu, CompleteMultipartUploadRequest request) {
        upload(mpu);
        rejectConditions(request.ifMatch(), request.ifNoneMatch());
        if (request.multipartUpload() == null ||
                request.multipartUpload().parts().isEmpty()) {
            throw S3Exceptions.invalidArgument(
                    "At least one multipart part is required");
        }
        var parts = new TreeMap<Integer, Part>();
        listMultipartUpload(mpu).forEach(part ->
                parts.put(part.partNumber(), part));
        Path temporary = temporaryFile();
        try {
            try (var output = Files.newOutputStream(temporary)) {
                int previous = 0;
                for (var completed : request.multipartUpload().parts()) {
                    Part part = parts.get(completed.partNumber());
                    if (part == null || !part.eTag().equals(
                            unquote(completed.eTag()))) {
                        throw S3Exceptions.invalidArgument(
                                "Multipart part or ETag does not match");
                    }
                    if (!completed.partNumber().equals(
                            request.multipartUpload().parts().get(
                                    request.multipartUpload().parts().size() -
                                            1).partNumber()) &&
                            part.size() < getMinimumMultipartPartSize()) {
                        throw S3Exceptions.invalidArgument(
                                "Nonfinal multipart part is too small");
                    }
                    if (completed.partNumber() <= previous) {
                        throw S3Exceptions.invalidArgument(
                                "Multipart parts must be in ascending order");
                    }
                    previous = completed.partNumber();
                    String partPath = item(uploadPath(mpu) + "/" +
                            part.partNumber());
                    JsonNode node = json("GET", partPath, null);
                    if (!etag(node).equals(unquote(completed.eTag()))) {
                        throw S3Exceptions.invalidArgument(
                                "Multipart part or ETag does not match");
                    }
                    try (var input = download(node)) {
                        input.transferTo(output);
                    }
                    // Item IDs survive overwrites; verify the revision after
                    // consuming the body, before publishing any staged bytes.
                    JsonNode current = json("GET", partPath, null);
                    if (!node.path("id").equals(current.path("id")) ||
                            !node.path("eTag").equals(current.path("eTag"))) {
                        throw S3Exceptions.invalidArgument(
                                "Multipart part changed during completion");
                    }
                }
            }
            var result = putBlob(PutObjectRequest.builder()
                    .bucket(mpu.containerName()).key(mpu.blobName())
                    .contentLength(Files.size(temporary)).build(),
                    Files.newInputStream(temporary));
            abortMultipartUpload(mpu);
            return CompleteMultipartUploadResponse.builder()
                    .bucket(mpu.containerName()).key(mpu.blobName())
                    .eTag(result.eTag()).build();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        } finally {
            deleteTemporary(temporary);
        }
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return 5L * 1024 * 1024;
    }

    private String uploadPath(MultipartUpload mpu) {
        try {
            UUID.fromString(mpu.id());
        } catch (IllegalArgumentException iae) {
            throw S3Exceptions.noSuchUpload();
        }
        return mpu.containerName() + "/" + MULTIPART + "/" + mpu.id();
    }

    private void upload(MultipartUpload mpu) {
        bucket(mpu.containerName());
        try {
            JsonNode target = json("GET",
                    item(uploadPath(mpu) + "/target"), null);
            if (!mpu.blobName().equals(readTarget(target))) {
                throw S3Exceptions.noSuchUpload();
            }
        } catch (S3Exception se) {
            if (se.statusCode() == 404) {
                throw S3Exceptions.noSuchUpload();
            }
            throw se;
        }
    }

    private String readTarget(JsonNode target) {
        try (var input = download(target)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private InputStream download(JsonNode node) {
        var response = send("GET", byId(node) + "/content",
                HttpRequest.BodyPublishers.noBody(), Map.of(), true);
        if (response.statusCode() == 302 ||
                response.statusCode() == 307) {
            String url = response.headers().firstValue("Location")
                    .orElseThrow();
            closeBody(response);
            response = send("GET", url, HttpRequest.BodyPublishers.noBody(),
                    Map.of(), false);
        }
        check(response);
        return response.body();
    }

    private JsonNode putFile(String path, @Nullable Long length,
            @Nullable String contentMD5, InputStream payload) {
        Path temporary = temporaryFile();
        MessageDigest digest = MD5.newDigest();
        try {
            try (var input = new DigestInputStream(payload, digest)) {
                Files.copy(input, temporary,
                        StandardCopyOption.REPLACE_EXISTING);
            }
            long size = Files.size(temporary);
            if (length != null && length != size) {
                throw S3Exceptions.invalidRequest(
                        "Content-Length does not match payload");
            }
            if (contentMD5 != null && !MessageDigest.isEqual(digest.digest(),
                    Base64.getDecoder().decode(contentMD5))) {
                throw S3Exceptions.fromStatusCode(400);
            }
            ensureFolder(path.substring(0, path.lastIndexOf('/')));
            removeEmptyDestinationFolder(path);
            if (size <= SIMPLE_UPLOAD_LIMIT) {
                return readJson(send("PUT", item(path) + "/content",
                        HttpRequest.BodyPublishers.ofFile(temporary),
                        Map.of("Content-Type", "application/octet-stream"),
                        true));
            }
            JsonNode session = json("POST",
                    item(path) + "/createUploadSession",
                    Map.of("item", Map.of("@microsoft.graph.conflictBehavior",
                            "replace")));
            String url = session.path("uploadUrl").asText();
            try (var input = Files.newInputStream(temporary)) {
                long offset = 0;
                JsonNode result = null;
                while (offset < size) {
                    byte[] fragment = input.readNBytes(FRAGMENT_SIZE);
                    long end = offset + fragment.length - 1;
                    result = readJson(send("PUT", url,
                            HttpRequest.BodyPublishers.ofByteArray(fragment),
                            Map.of("Content-Range", "bytes " + offset + "-" +
                                    end + "/" + size, "Content-Type",
                                    "application/octet-stream"), false));
                    offset = end + 1;
                }
                if (result == null || !result.has("id")) {
                    throw new IllegalStateException(
                            "Graph upload did not complete");
                }
                return result;
            } catch (IOException | RuntimeException failure) {
                try {
                    var response = send("DELETE", url,
                            HttpRequest.BodyPublishers.noBody(),
                            Map.of(), false);
                    if (response.statusCode() == 404) {
                        closeBody(response);
                    } else {
                        check(response);
                        closeBody(response);
                    }
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        } finally {
            deleteTemporary(temporary);
        }
    }

    private void removeEmptyDestinationFolder(String path) {
        JsonNode node;
        try {
            node = json("GET", item(path), null);
        } catch (S3Exception se) {
            if (se.statusCode() == 404) {
                return;
            }
            throw se;
        }
        if (!node.has("folder")) {
            return;
        }
        var folders = new ArrayList<JsonNode>();
        collectEmptyFolders(node, folders);
        for (JsonNode folder : folders) {
            JsonNode current = json("GET", byId(folder), null);
            String tag = folderContentTag(current);
            if (!children(byId(current)).isEmpty()) {
                throw S3Exceptions.invalidRequest(
                        "A nonempty folder occupies the object key");
            }
            // Only cTag guards descendant changes during recursive deletion.
            readJson(send("DELETE", byId(current),
                    HttpRequest.BodyPublishers.noBody(),
                    Map.of("If-Match", tag), true));
        }
    }

    private static String folderContentTag(JsonNode folder) {
        String tag = folder.path("cTag").asText();
        if (tag.isEmpty()) {
            throw S3Exceptions.invalidRequest(
                    "Cannot safely replace a folder without a " +
                            "descendant-change tag");
        }
        return tag;
    }

    private void collectEmptyFolders(JsonNode folder, List<JsonNode> folders) {
        folderContentTag(folder);
        for (JsonNode child : children(byId(folder))) {
            if (!child.has("folder")) {
                throw S3Exceptions.invalidRequest(
                        "A nonempty folder occupies the object key");
            }
            collectEmptyFolders(child, folders);
        }
        folders.add(folder);
    }

    private JsonNode bucket(String container) {
        validateSegment(container);
        try {
            JsonNode node = json("GET", item(container), null);
            if (!node.has("folder")) {
                throw S3Exceptions.noSuchBucket(container, null);
            }
            return node;
        } catch (S3Exception se) {
            if (se.statusCode() == 404) {
                throw S3Exceptions.noSuchBucket(container, null, se);
            }
            throw se;
        }
    }

    private JsonNode object(String container, String key) {
        validateKey(key);
        bucket(container);
        try {
            JsonNode node = json("GET", item(container + "/" + key), null);
            if (!node.has("file")) {
                throw S3Exceptions.noSuchKey(container, key, null);
            }
            return node;
        } catch (S3Exception se) {
            if (se.statusCode() == 404) {
                throw S3Exceptions.noSuchKey(container, key, null, se);
            }
            throw se;
        }
    }

    private void ensureFolder(String path) {
        String[] segments = path.split("/", -1);
        String parent = byId(bucket(segments[0]));
        for (int index = 1; index < segments.length; ++index) {
            String segment = segments[index];
            String next = parent + ":/" + encode(segment) + ":";
            JsonNode node;
            try {
                node = json("GET", next, null);
            } catch (S3Exception se) {
                if (se.statusCode() != 404) {
                    throw se;
                }
                try {
                    node = createFolder(parent, segment);
                } catch (S3Exception conflict) {
                    if (conflict.statusCode() != 409) {
                        throw conflict;
                    }
                    node = json("GET", next, null);
                }
            }
            if (!node.has("folder")) {
                throw S3Exceptions.invalidRequest(
                        "An object occupies a parent folder");
            }
            parent = byId(node);
        }
    }

    private JsonNode createFolder(String parent, String name) {
        return json("POST", parent + "/children",
                Map.of("name", name, "folder", Map.of(),
                        "@microsoft.graph.conflictBehavior", "fail"));
    }

    private List<JsonNode> children(String folder) {
        var result = new ArrayList<JsonNode>();
        String next = folder + "/children";
        while (next != null) {
            JsonNode page = json("GET", next, null);
            page.path("value").forEach(result::add);
            next = page.has("@odata.nextLink") ?
                    page.path("@odata.nextLink").asText() : null;
        }
        return result;
    }

    private String item(String path) {
        String base = endpoint + drive + root;
        if (path.isEmpty()) {
            return base;
        }
        return base + ":/" + Arrays.stream(path.split("/", -1))
                .map(OneDriveBlobStore::encode)
                .collect(Collectors.joining("/")) + ":";
    }

    private String byId(JsonNode node) {
        return endpoint + drive + "/items/" +
                encode(node.path("id").asText());
    }

    private JsonNode json(String method, String url,
            @Nullable Object body) {
        // The closing colon separates a path from /content or /children;
        // metadata requests end at the path itself.
        if (url.endsWith(":")) {
            url = url.substring(0, url.length() - 1);
        }
        try {
            var publisher = body == null ?
                    HttpRequest.BodyPublishers.noBody() :
                    HttpRequest.BodyPublishers.ofByteArray(
                            MAPPER.writeValueAsBytes(body));
            return readJson(send(method, url, publisher,
                    Map.of("Content-Type", "application/json"), true));
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private HttpResponse<InputStream> send(String method, String url,
            HttpRequest.BodyPublisher body, Map<String, String> headers,
            boolean authenticated) {
        URI uri = URI.create(url);
        validateUrl(uri);
        var builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMinutes(5)).method(method, body);
        if (authenticated) {
            if (!endpoint.getScheme().equals(uri.getScheme()) ||
                    !endpoint.getAuthority().equals(uri.getAuthority()) ||
                    !uri.getPath().startsWith(endpoint.getPath() + "/")) {
                throw new IllegalArgumentException(
                        "Graph link is outside the configured endpoint");
            }
            builder.header("Authorization", "Bearer " + token());
        }
        headers.forEach(builder::header);
        try {
            return client.send(builder.build(), BodyHandlers.ofInputStream());
        } catch (IOException ioe) {
            throw new UncheckedIOException("OneDrive request failed", ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("OneDrive request interrupted", ie);
        }
    }

    private String token() {
        String token = creds.get().sessionToken();
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException(
                    "OneDrive requires jclouds.session-token");
        }
        return token;
    }

    private static JsonNode readJson(HttpResponse<InputStream> response) {
        check(response);
        try (var input = response.body()) {
            JsonNode node = MAPPER.readTree(input);
            return node == null ? MAPPER.createObjectNode() : node;
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private static void check(HttpResponse<InputStream> response) {
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            closeBody(response);
            // A Graph 400 is InvalidRequest, not the frontend's BadDigest.
            if (status == 400) {
                throw S3Exceptions.invalidRequest(
                        "Microsoft Graph rejected the request");
            }
            throw S3Exceptions.fromStatusCode(status);
        }
    }

    private static void closeBody(HttpResponse<InputStream> response) {
        try {
            response.body().close();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private static GetObjectResponse metadata(JsonNode node) {
        return GetObjectResponse.builder()
                .contentLength(node.path("size").asLong())
                .contentType(node.path("file").path("mimeType")
                        .asText("application/octet-stream"))
                .eTag(etag(node)).lastModified(modified(node)).build();
    }

    private static String etag(JsonNode node) {
        // A dash prevents S3 clients interpreting a Graph ETag as an MD5.
        return HexFormat.of().formatHex(node.path("eTag").asText()
                .getBytes(StandardCharsets.UTF_8)) + "-1";
    }

    private static Instant modified(JsonNode node) {
        return Instant.parse(node.path("lastModifiedDateTime").asText());
    }

    private static Instant created(JsonNode node) {
        return Instant.parse(node.path("createdDateTime").asText());
    }

    private static String unquote(String value) {
        return value.startsWith("\"") && value.endsWith("\"") ?
                value.substring(1, value.length() - 1) : value;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    private static void validateKey(String key) {
        for (String segment : key.split("/", -1)) {
            validateSegment(segment);
        }
        if (MULTIPART.equalsIgnoreCase(key.split("/", 2)[0])) {
            throw S3Exceptions.invalidArgument(
                    "The .s3proxy-multipart prefix is reserved");
        }
    }

    private static void validateSegment(String segment) {
        if (segment.isEmpty() || ".".equals(segment) ||
                "..".equals(segment) || segment.endsWith(".") ||
                segment.startsWith(" ") || segment.endsWith(" ") ||
                segment.chars().anyMatch(character ->
                        character < 32 || "\"*:<>?\\|/".indexOf(
                                character) >= 0)) {
            throw S3Exceptions.invalidArgument(
                    "Name is not supported by OneDrive");
        }
    }

    private static void validateUrl(URI uri) {
        if (uri.getHost() == null || uri.getUserInfo() != null ||
                uri.getFragment() != null ||
                !("https".equals(uri.getScheme()) ||
                ("http".equals(uri.getScheme()) &&
                ("127.0.0.1".equals(uri.getHost()) ||
                "localhost".equals(uri.getHost()))))) {
            throw new IllegalArgumentException(
                    "OneDrive URLs must use HTTPS (or localhost for tests)");
        }
    }

    private static void privateAccess(boolean allowed) {
        if (!allowed) {
            throw new UnsupportedOperationException(
                    "OneDrive only supports private ACLs");
        }
    }

    private static void rejectEncryption(@Nullable String algorithm,
            @Nullable String customerAlgorithm) {
        if (algorithm != null || customerAlgorithm != null) {
            throw new UnsupportedOperationException(
                    "OneDrive does not support S3 server-side encryption");
        }
    }

    private static void rejectVersion(@Nullable String version) {
        if (version != null) {
            throw new UnsupportedOperationException(
                    "OneDrive does not support S3 versioning");
        }
    }

    private static void rejectConditions(@Nullable String ifMatch,
            @Nullable String ifNoneMatch) {
        if (ifMatch != null || ifNoneMatch != null) {
            throw new UnsupportedOperationException(
                    "OneDrive conditional writes are not supported");
        }
    }

    private static Path temporaryFile() {
        try {
            return Files.createTempFile("s3proxy-onedrive-", ".tmp");
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private static void deleteTemporary(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}
