package awss3reader_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awss3reader "github.com/nikolaydubina/aws-s3-reader"
)

func TestS3ReadSeeker(t *testing.T) {
	mySession := session.Must(session.NewSession(
		aws.NewConfig().WithRegion("ap-southeast-1"),
	))
	s3client := s3.New(mySession)

	bucket := "nikolaydubina-blog-public"
	key := "photos/2021-12-20-4.jpeg"

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		bucket,
		key,
		1<<10*100,
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
		os.WriteFile("got", got, 0644)
		t.Errorf("expected %d bytes, got %d", len(exp), len(got))
	}
}

func TestS3ReadSeeker_Seek_Current(t *testing.T) {
	mySession := session.Must(session.NewSession(
		aws.NewConfig().WithRegion("ap-southeast-1"),
	))
	s3client := s3.New(mySession)

	bucket := "nikolaydubina-blog-public"
	key := "photos/2021-12-20-4.jpeg"

	r := awss3reader.NewS3ReadSeeker(
		s3client,
		bucket,
		key,
		1<<10*100,
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
		os.WriteFile("got", got, 0644)
		t.Errorf("expected %d bytes, got %d", len(exp), len(got))
	}
}
