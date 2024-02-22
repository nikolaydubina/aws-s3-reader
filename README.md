#### AWS S3 Reader

[![codecov](https://codecov.io/gh/nikolaydubina/aws-s3-reader/graph/badge.svg?token=RjrAU7oJgH)](https://codecov.io/gh/nikolaydubina/aws-s3-reader)
[![Go Report Card](https://goreportcard.com/badge/github.com/nikolaydubina/aws-s3-reader)](https://goreportcard.com/report/github.com/nikolaydubina/aws-s3-reader)

Efficient reader for large S3 files.

* `Seek()` via `Byte-Range` HTTP offsets[^1][^2]
* zero-memory copy
* early HTTP Body termination

#### Related Work

* https://github.com/yacchi/s3-fast-reader — provides `io.Reader` interface, focuses on connection pool and parallelism, uses mocks for tests

[^1]: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html
[^2]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests
