package run

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/echa/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	s3endpoint, s3user, s3secret, s3bucket string
	skipUpload                             bool
	defaultSeeds                           []uint64
)

func init() {
	defaultSeeds = config.GetUint64Slice("DST_SEEDS")
	skipUpload = config.GetBool(os.Getenv("SKIP_UPLOAD"))
	s3user = os.Getenv("MINIO_USER")
	s3bucket = os.Getenv("MINIO_BUCKET")
	s3secret = os.Getenv("MINIO_SECRET")
	s3endpoint = os.Getenv("MINIO_URL")
}

func InitStorage(t *testing.T) (*minio.Client, error) {
	t.Helper()
	if s3endpoint == "" {
		t.Logf("Missing s3 url, disabling file upload. Set MINIO_URL to enable.")
		skipUpload = true
		return nil, nil
	}
	if s3bucket == "" {
		t.Logf("Missing s3 bucket, disabling file upload. Set MINIO_BUCKET to enable.")
		skipUpload = true
		return nil, nil
	}
	if s3user == "" || s3secret == "" {
		return nil, fmt.Errorf("Missing S3 credentails, set MINIO_USER and MINIO_SECRET.")
	}
	s3, err := minio.New(
		s3endpoint,
		&minio.Options{
			Creds:  credentials.NewStaticV4(s3user, s3secret, ""),
			Secure: true,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		})
	if err != nil {
		return nil, err
	}
	return s3, nil
}
