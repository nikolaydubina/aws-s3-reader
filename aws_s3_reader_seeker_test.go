package awss3reader_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awss3reader "github.com/nikolaydubina/aws-s3-reader"
)

func TestS3ReadSeeker(t *testing.T) {
	mySession := session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion("ap-southeast-1").
			WithCredentials(credentials.AnonymousCredentials),
	))
	s3client := s3.New(mySession)

	bucket := "nikolaydubina-blog-public"
	key := "photos/2021-12-20-4.jpeg"

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		bucket,
		key,
		awss3reader.FixedChunkSizePolicy{Size: 1 << 10 * 100}, // 100 KB
	)
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	downloader := s3manager.NewDownloader(mySession)
	f, err := os.CreateTemp("", "s3reader")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	n, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	exp, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(exp)) {
		t.Errorf("expected %d bytes, got %d", len(exp), n)
	}

	if !bytes.Equal(exp, got) {
		t.Errorf("expected %d bytes, got %d", len(exp), len(got))
	}
}

func TestS3ReadSeeker_SeekLarge(t *testing.T) {
	mySession := session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion("ap-southeast-1").
			WithCredentials(credentials.AnonymousCredentials),
	))
	s3client := s3.New(mySession)

	bucket := "nikolaydubina-blog-public"
	key := "photos/2021-12-20-4.jpeg"

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		bucket,
		key,
		awss3reader.FixedChunkSizePolicy{Size: 1 << 10 * 100}, // 100 KB
	)
	defer r.Close()

	var offset int64 = 1 << 10 * 100
	r.Seek(offset+100, io.SeekCurrent)
	r.Seek(offset, io.SeekStart)
	r.Seek(0, io.SeekCurrent)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	downloader := s3manager.NewDownloader(mySession)
	f, err := os.CreateTemp("", "s3reader")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	n, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	exp, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(exp)) {
		t.Errorf("expected %d bytes, got %d", len(exp), n)
	}

	if !bytes.Equal(exp[offset:], got) {
		t.Errorf("expected %d bytes, got %d", len(exp), len(got))
	}
}

func TestS3ReadSeeker_SeekDiscardHTTPBody(t *testing.T) {
	mySession := session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion("ap-southeast-1").
			WithCredentials(credentials.AnonymousCredentials),
	))
	s3client := s3.New(mySession)

	bucket := "nikolaydubina-blog-public"
	key := "photos/2021-12-20-4.jpeg"

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		bucket,
		key,
		awss3reader.FixedChunkSizePolicy{Size: 1 << 10 * 100}, // 100 KB
	)
	defer r.Close()

	got1, err := io.ReadAll(io.LimitReader(r, 100))
	if err != nil {
		t.Fatal(err)
	}

	n, err := r.Seek(100, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if n != 200 {
		t.Errorf("expected 200 offset, got %d", n)
	}

	got2, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	downloader := s3manager.NewDownloader(mySession)
	f, err := os.CreateTemp("", "s3reader")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	n, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	exp, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(exp)) {
		t.Errorf("expected %d bytes, got %d", len(exp), n)
	}

	if !bytes.Equal(exp[:100], got1) {
		t.Errorf("expected %d bytes, got %d", len(exp), len(got1))
	}
	if !bytes.Equal(exp[200:], got2) {
		t.Errorf("expected %d bytes, got %d", len(exp), len(got1))
	}
}

func TestS3ReadSeeker_NotFoundObject(t *testing.T) {
	mySession := session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion("ap-southeast-1").
			WithCredentials(credentials.AnonymousCredentials),
	))
	s3client := s3.New(mySession)

	bucket := "nikolaydubina-blog-public"
	key := "something-something"

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		bucket,
		key,
		awss3reader.FixedChunkSizePolicy{Size: 1 << 10 * 100}, // 100 KB
	)
	defer r.Close()

	if _, err := r.Seek(100, io.SeekEnd); err == nil {
		t.Errorf("expected error, got nil")
	}

	if _, err := io.ReadAll(r); err != nil {
		t.Errorf("expected no error")
	}
}

func ExampleS3ReadSeeker() {
	s3client := s3.New(session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion("ap-southeast-1").
			WithCredentials(credentials.AnonymousCredentials),
	)))

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		"nikolaydubina-blog-public",
		"videos/2024-02-22.mov",
		awss3reader.FixedChunkSizePolicy{Size: 1 << 20 * 40},
	)
	defer r.Close()

	r.Seek(100, io.SeekCurrent)
	res, err := io.ReadAll(r)

	if err != nil || len(res) == 0 {
		panic(err)
	}
}
func TestS3ReadSeeker_Seek(t *testing.T) {
	mySession := session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion("ap-southeast-1").
			WithCredentials(credentials.AnonymousCredentials),
	))
	s3client := s3.New(mySession)

	bucket := "nikolaydubina-blog-public"
	key := "photos/2021-12-20-4.jpeg"

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		bucket,
		key,
		awss3reader.FixedChunkSizePolicy{Size: 1 << 10 * 100}, // 100 KB
	)
	defer r.Close()

	// Seek to offset 100 from current position
	offset, err := r.Seek(100, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Errorf("expected offset 100, got %d", offset)
	}

	// Seek to offset 200 from start
	offset, err = r.Seek(200, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 200 {
		t.Errorf("expected offset 200, got %d", offset)
	}

	// Seek to current position (0 offset)
	offset, err = r.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 200 {
		t.Errorf("expected offset 200, got %d", offset)
	}

	// Seek beyond end of file
	_, err = r.Seek(1000, io.SeekEnd)
	if err == nil {
		t.Errorf("expected error, got nil when seeking beyond end of file")
	}

	// seek 0 from end
	offset, err = r.Seek(0, io.SeekEnd)
	if err != nil {
		t.Errorf("expected nil, got %v when seeking 0 from end", err)
	}
	if offset != 11356322 {
		t.Errorf("expected offset 11356322, got %d", offset)
	}

	// seek -100 from end
	offset, err = r.Seek(-100, io.SeekEnd)
	if err != nil {
		t.Errorf("expected nil, got %v when seeking -100 from end", err)
	}

	if offset != 11356222 {
		t.Errorf("expected offset 11356222, got %d", offset)
	}
}
