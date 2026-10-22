# OneDrive

Status: experimental

The OneDrive backend exposes a Microsoft Graph drive (OneDrive for Business
or a SharePoint document library) as an S3Proxy storage
backend through S3Proxy's `BlobStore` interface.

## Security

Use Microsoft Entra application credentials with the appropriate Microsoft Graph
**application permissions** and administrator consent for the target drive.
The automatically acquired access token is sent only to the
configured Graph endpoint; Graph pagination links and upload or download URLs
outside it are refused or fetched without the token.

Keep S3Proxy client credentials separate from the Graph token. S3 clients
authenticate to S3Proxy with `s3proxy.identity` and `s3proxy.credential`;
S3Proxy authenticates to Graph with access tokens acquired automatically using
application credentials. Static access tokens and delegated authentication
are not supported.

Graph endpoints must use HTTPS (plain HTTP is accepted only for `localhost`
and `127.0.0.1`, for tests).

## Configuration

### Application authentication (automatic token renewal)

For unattended OneDrive for Business or SharePoint deployments, [register an
application in the drive's Microsoft Entra tenant](https://learn.microsoft.com/en-us/graph/auth-register-app-v2). Grant Microsoft Graph
**application** permissions for the required read/write operations and obtain
administrator consent. For example, `Files.ReadWrite.All` permits broad access;
use a narrower supported permission and resource-specific grant where possible.
Delegated permissions such as `Files.ReadWrite` are not sufficient for this
flow. *Personal OneDrive accounts are not supported by application authentication*.

Configure the connector with:

```properties
s3proxy.authorization=aws-v2-or-v4
s3proxy.endpoint=http://127.0.0.1:8080
s3proxy.identity=local-identity
s3proxy.credential=local-credential

jclouds.provider=onedrive
jclouds.identity=DRIVE_ID
s3proxy.onedrive.tenant-id=TENANT_ID
s3proxy.onedrive.client-id=APPLICATION_CLIENT_ID
s3proxy.onedrive.client-secret=CLIENT_SECRET_VALUE
# Optional: existing folder item ID; omitted uses the drive root.
# s3proxy.onedrive.root=FOLDER_ITEM_ID
# Optional: Graph API endpoint, for example for a sovereign cloud.
# jclouds.endpoint=https://graph.microsoft.com/v1.0
```

Use the client secret's **value**, not its secret ID. An explicit drive ID is
required: application tokens cannot use `/me/drive`. Remove
`jclouds.session-token` from any previous configuration; nonempty values are
rejected. Tenant ID, client ID, client secret, and drive ID are mandatory.

The connector uses the existing Azure Identity library's client-credentials
flow. Tokens are cached in memory per connector and renewed on demand when
within one minute of expiry; concurrent requests share renewal. No refresh
token is required, and access tokens need not be stored in Kubernetes.
Authentication failures fail the operation explicitly instead of continuing
with an expired token. Credentials are validated by acquiring a token during
connector initialization.

The default authority is `https://login.microsoftonline.com`. For a sovereign
cloud, configure `s3proxy.onedrive.authority-host` with the corresponding HTTPS
login origin and `jclouds.endpoint` with that cloud's Graph API endpoint.
The requested scope is the Graph endpoint's origin followed by `/.default`.
Application permissions and consent must exist in that cloud.

Client secrets still expire and can be revoked. Rotate the secret before its
expiry and restart the pods; automatic access-token renewal does not rotate
the client secret.

### Kubernetes application credentials

Keep the S3 client credentials separate from the application credentials.
For example, create a Secret in the S3Proxy deployment's namespace:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3proxy-credentials
type: Opaque
stringData:
  s3proxy-identity: "<S3 access key>"
  s3proxy-credential: "<S3 secret key>"
  onedrive-client-secret: "<Entra client secret value>"
```

Do not commit populated Secrets. Restrict access with Kubernetes RBAC and
enable encryption at rest, or populate the Secret through an external secret
manager. Tenant ID, client ID, and drive ID are configuration, not passwords.

Using the Secret volume from the
[cloud-backend deployment](../examples/kubernetes/cloud-backend/deployment.yaml),
replace its `env` configuration with:

```yaml
env:
  - name: JCLOUDS_PROVIDER
    value: onedrive
  - name: JCLOUDS_IDENTITY
    value: "<drive ID>"
  - name: S3PROXY_ONEDRIVE_TENANT_ID
    value: "<tenant ID>"
  - name: S3PROXY_ONEDRIVE_CLIENT_ID
    value: "<application client ID>"
  - name: S3PROXY_ONEDRIVE_CLIENT_SECRET_FILE
    value: /run/secrets/s3proxy/onedrive-client-secret
  - name: S3PROXY_IDENTITY_FILE
    value: /run/secrets/s3proxy/s3proxy-identity
  - name: S3PROXY_CREDENTIAL_FILE
    value: /run/secrets/s3proxy/s3proxy-credential
```

Remove the example's `JCLOUDS_IDENTITY_FILE` and `JCLOUDS_CREDENTIAL_FILE`;
do not set `JCLOUDS_SESSION_TOKEN` or `JCLOUDS_SESSION_TOKEN_FILE` in this mode.
Use an image built with application-authentication support.

The container also accepts `S3PROXY_ONEDRIVE_CLIENT_SECRET` directly, but the
`_FILE` variant avoids putting the secret value in the pod specification.
Tenant and client IDs also support `_FILE` variants. Optional configuration
uses `S3PROXY_ONEDRIVE_ROOT`, `S3PROXY_ONEDRIVE_AUTHORITY_HOST`, and
`JCLOUDS_ENDPOINT`. Secret files are read at startup only; after updating the
client secret, restart the deployment:

```shell
kubectl rollout restart deployment/s3proxy
```

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
objects only when Graph supplies folder `cTag` values; nonempty directories
are never replaced. Empty-directory removal uses `If-Match` with the folder's
`cTag`, which tracks descendant changes. Folder `eTag` values do not provide
this protection. Concurrent changes cause the upload to fail. If any folder
in the destination tree lacks a `cTag`, replacement is refused before any
folders are deleted. This includes OneDrive for Business and SharePoint
document libraries, which do not return folder `cTag` values.

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
