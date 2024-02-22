#### AWS S3 Reader

Efficient reader for large S3 files.

* `Seek()` via `Byte-Range` HTTP offsets[^1][^2]
* zero-memory copy
* early HTTP Body termination

[^1]: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html
[^2]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests
