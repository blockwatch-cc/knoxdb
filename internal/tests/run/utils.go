package run

import (
	"fmt"
	"os"

	"github.com/echa/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	s3endpoint, s3user, s3secret, s3bucket string
	skipUpload                             bool
	defaultIter                            []uint64
)

func init() {
	defaultIter = config.GetUint64Slice("DEFAULT_ITERS")
	skipUpload = config.GetBool(os.Getenv("SKIP_UPLOAD"))
	s3user = requireEnv("MINIO_USER")
	s3bucket = requireEnv("MINIO_BUCKET")
	s3secret = requireEnv("MINIO_SECRET")
	s3endpoint = requireEnv("MINIO_URL")
}

func requireEnv(name string) string {
	s := os.Getenv(name)
	if s == "" {
		panic(fmt.Errorf("Missing env var %s", name))
	}
	return s
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
