# OneDrive

Status: experimental

The OneDrive backend exposes a Microsoft Graph drive (personal OneDrive,
OneDrive for Business or a SharePoint document library) as an S3Proxy storage
backend through S3Proxy's `BlobStore` interface.

## Security

Use a Microsoft Graph access token with read/write access to the target drive
(delegated `Files.ReadWrite`, or the appropriate application permissions and
administrator consent for the chosen drive). The token is sent only to the
configured Graph endpoint; Graph pagination links and upload or download URLs
outside it are refused or fetched without the token.

Keep S3Proxy client credentials separate from the Graph token. S3 clients
authenticate to S3Proxy with `s3proxy.identity` and `s3proxy.credential`;
S3Proxy authenticates to Graph with `jclouds.session-token`.

Graph endpoints must use HTTPS (plain HTTP is accepted only for `localhost`
and `127.0.0.1`, for tests).

## Configuration

Example:

```
s3proxy.authorization=aws-v2-or-v4
s3proxy.endpoint=http://127.0.0.1:8080
s3proxy.identity=local-identity
s3proxy.credential=local-credential

jclouds.provider=onedrive
jclouds.session-token=MICROSOFT_GRAPH_ACCESS_TOKEN
# Optional: drive ID; omitted uses /me/drive (delegated tokens only).
# jclouds.identity=DRIVE_ID
# Optional: existing folder item ID; omitted uses the drive root.
# s3proxy.onedrive.root=FOLDER_ITEM_ID
# Optional: Graph API endpoint, for example for a sovereign cloud.
# jclouds.endpoint=https://graph.microsoft.com/v1.0
```

The backend does not perform interactive sign-in or refresh a static token.
For long-running deployments, embed S3Proxy with a `Supplier<Credentials>`
that renews the access token; the backend reads it again for every request.
Tokens must target the configured Graph cloud.

## Storage Mapping

Buckets map to folders underneath the selected root; objects map to files
and slash-separated subfolders. The effective path mapping is:

```
<root>/<bucket>/<object-key>
```

Use a dedicated root folder: existing folders and files there are exposed as
buckets and objects.

OneDrive names are case-insensitive and subject to OneDrive's path and size
restrictions. Empty path segments, `.`/`..`, directory-marker keys ending in
`/`, trailing dots, leading/trailing spaces, control characters and
`"*:<>?\|` are rejected. An object cannot also be a parent directory (`a` and
`a/b`). An upload can replace an empty directory tree left by deleted
objects; nonempty directories are never replaced. Empty-directory removal is
guarded by Graph ETags, and concurrent changes cause the upload to fail.

The `.s3proxy-multipart/` prefix is reserved within every bucket.

## Uploads and Copies

Uploads are staged on local temporary disk, then sent to Graph; files larger
than 4 MiB use upload sessions with bounded-memory fragments. Multipart parts
and their target are kept in OneDrive, survive proxy restarts, and are hidden
from object listings. Completion stages the selected parts on local disk;
allow enough temporary space for the assembled object and its upload staging
copy. Completion revalidates each requested part ETag before downloading and
checks the item ID and Graph ETag again after staging its content. A concurrent
part replacement fails completion without publishing the object or deleting
the multipart upload state. CopyObject and UploadPartCopy stream through the
proxy rather than performing an asynchronous Graph copy.

## Metadata

Only private S3 ACLs are supported; they do not alter any sharing permissions
already configured in OneDrive. User metadata, flexible checksums and
Content-* headers supplied on upload are not persisted; reads use Graph's
detected MIME type, modification time and opaque ETag (not the object's MD5).
S3 versioning, S3 server-side-encryption requests, and conditional writes,
copies and deletes are not supported.

Conditional GET and HEAD requests evaluate `If-Match`, `If-None-Match`,
`If-Modified-Since` and `If-Unmodified-Since` against the object's Graph
metadata before any content is downloaded.
Not-modified conditions return HTTP 304; failed preconditions return HTTP
412, including when a request combines conditional headers.

Downloads (including range requests) are staged on local temporary disk before
being returned, then the item ID and Graph ETag are checked again. Concurrent
changes discard the staged content and retry with fresh metadata and conditional
checks, up to three download attempts. Persistent changes return HTTP 503.
Allow temporary disk space for each concurrent download; its staged file is
removed when the response stream is closed.

Listings traverse every Graph page and sort locally to provide S3 prefix,
delimiter and continuation-token semantics, so listing a large bucket costs
one Graph request per folder page.

## Testing

`OneDriveBlobStoreTest` runs the backend, and S3 clients through S3Proxy,
against a local Microsoft Graph fixture, so it needs no account and runs with
the rest of `mvn test`. Unlike the other backends, the full `AwsSdkTest`
suite does not run against OneDrive in CI: there is no Graph emulator to
point it at. Validate against a real drive before production use.
