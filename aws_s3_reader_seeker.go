package awss3reader

import (
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ChunkSizePolicy is something that can tell how much data to fetch in single request for given S3 Object.
// With more advanced policies, Visit methods will be integrated.
type ChunkSizePolicy interface {
	ChunkSize() int
}

// FixedChunkSizePolicy always returns same chunk size.
type FixedChunkSizePolicy struct {
	Size int
}

func (s FixedChunkSizePolicy) ChunkSize() int { return s.Size }

// S3ReadSeeker is a reader of given S3 Object.
// It utilizes HTTP Byte Ranges to read chunks of data from S3 Object.
// It uses zero-memory copy from underlying HTTP Body response.
// It uses early HTTP Body termination, if seeks are beyond current HTTP Body.
// It uses adaptive policy for chunk size fetching.
// This is useful for iterating over very large S3 Objects.
type S3ReadSeeker struct {
	s3client        *s3.S3
	bucket          string
	key             string
	offset          int64 // in s3 object
	size            int64 // in s3 object
	lastByte        int64 // in s3 object that we expect to have in current HTTP Body
	chunkSizePolicy ChunkSizePolicy
	r               io.ReadCloser // temporary holder for current reader
	sink            []byte        // where to read bytes discarding data from readers during in-body seek
}

func NewS3ReadSeeker(
	s3client *s3.S3,
	bucket string,
	key string,
	minChunkSize int,
	chunkSizePolicy ChunkSizePolicy,
) *S3ReadSeeker {
	return &S3ReadSeeker{
		s3client:        s3client,
		bucket:          bucket,
		key:             key,
		chunkSizePolicy: chunkSizePolicy,
		sink:            make([]byte, minChunkSize),
	}
}

// Seek assumes always can seek to position in S3 object.
// Seeking beyond S3 file size will result failures in Read calls.
func (s *S3ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	discardBytes := 0

	switch whence {
	case io.SeekCurrent:
		discardBytes = int(offset)
		s.offset += offset
	case io.SeekStart:
		// seeking backwards results in dropping current http body.
		// since http body reader can read only forwards.
		if offset < s.offset {
			s.reset()
		}
		discardBytes = int(offset - s.offset)
		s.offset = offset
	default:
		return 0, errors.New("unsupported whence")
	}

	if s.offset > s.lastByte {
		s.reset()
		discardBytes = 0
	}

	if discardBytes > 0 {
		// not seeking
		if discardBytes > len(s.sink) {
			s.sink = make([]byte, discardBytes)
		}
		n, err := s.r.Read(s.sink[:discardBytes])
		if err != nil || n < discardBytes {
			s.reset()
		}
	}

	return s.offset, nil
}

func (s *S3ReadSeeker) Close() error {
	if s.r != nil {
		return s.r.Close()
	}
	return nil
}

func (s *S3ReadSeeker) Read(b []byte) (int, error) {
	if s.r == nil {
		if err := s.fetch(s.chunkSizePolicy.ChunkSize()); err != nil {
			return 0, err
		}
	}

	n, err := s.r.Read(b)
	s.offset += int64(n)

	if err != nil && errors.Is(err, io.EOF) {
		return n, s.fetch(s.chunkSizePolicy.ChunkSize())
	}

	return n, err
}

func (s *S3ReadSeeker) reset() {
	if s.r != nil {
		s.r.Close()
	}
	s.r = nil
	s.lastByte = 0
}

func (s *S3ReadSeeker) getSize() int {
	if s.size > 0 {
		return int(s.size)
	}
	resp, err := s.s3client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		return 0
	}
	s.size = *resp.ContentLength
	return int(s.size)
}

func (s *S3ReadSeeker) fetch(n int) error {
	s.reset()

	n = min(n, s.getSize()-int(s.offset))
	if n <= 0 {
		return io.EOF
	}

	// note, that HTTP Byte Ranges is inclusive range of start-byte and end-byte
	s.lastByte = s.offset + int64(n) - 1
	resp, err := s.s3client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", s.offset, s.lastByte)),
	})
	if err != nil {
		return fmt.Errorf("cannot fetch bytes=%d-%d: %w", s.offset, s.lastByte, err)
	}
	s.r = resp.Body
	return nil
}
