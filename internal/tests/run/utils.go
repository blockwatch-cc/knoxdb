package run

import (
	"os"

	"github.com/echa/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	s3endpoint, s3user, s3secret, s3bucket, path string
	skipUpload                                   bool
	defaultIter                                  []uint64
)

func init() {
	defaultIter = config.GetUint64Slice("DEFAULT_ITERS")
	skipUpload = config.GetBool(os.Getenv("SKIP_UPLOAD"))
	path = os.Getenv("LOGS_PATH")
	s3user = os.Getenv("MINIO_USER")
	s3bucket = os.Getenv("MINIO_BUCKET")
	s3secret = os.Getenv("MINIO_SECRET")
	s3endpoint = os.Getenv("MINIO_URL")
}

func LoadStorage() (*minio.Client, error) {
	s3, err := minio.New(
		s3endpoint,
		&minio.Options{
			Creds:  credentials.NewStaticV4(s3user, s3secret, ""),
			Secure: true,
		})
	if err != nil {
		return nil, err
	}
	return s3, nil
}
