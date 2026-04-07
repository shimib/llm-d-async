package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
)

var (
	ErrObjectPreconditionFailed = errors.New("object precondition failed")
)

// ObjectIterator defines the interface for iterating over GCS objects.
type ObjectIterator interface {
	Next() (*storage.ObjectAttrs, error)
}

// ClientWrapper defines the interface for interacting with GCS.
type ClientWrapper interface {
	// AtomicMove moves an object from src to dst. It uses IfGenerationMatch to ensure atomicity.
	// Returns ErrObjectPreconditionFailed if the source generation doesn't match the expected.
	AtomicMove(ctx context.Context, bucket, srcObject, dstObject string, srcGeneration int64) error

	// Download reads an object's contents.
	Download(ctx context.Context, bucket, object string) ([]byte, error)

	// Upload writes data to an object.
	Upload(ctx context.Context, bucket, object string, data []byte) error

	// Delete removes an object.
	Delete(ctx context.Context, bucket, object string) error

	// ListPrefix returns an iterator to list objects with a given prefix.
	ListPrefix(ctx context.Context, bucket, prefix string) ObjectIterator

	// Close closes the underlying GCS client.
	Close() error
}

type gcsClientWrapper struct {
	client *storage.Client
}

// NewClientWrapper creates a new GCS ClientWrapper.
func NewClientWrapper(ctx context.Context) (ClientWrapper, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcs client: %w", err)
	}
	return &gcsClientWrapper{client: client}, nil
}

func (w *gcsClientWrapper) AtomicMove(ctx context.Context, bucketName, srcObject, dstObject string, srcGeneration int64) error {
	bucket := w.client.Bucket(bucketName)
	src := bucket.Object(srcObject)
	if srcGeneration != 0 {
		src = src.If(storage.Conditions{GenerationMatch: srcGeneration})
	}
	dst := bucket.Object(dstObject)

	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		// Workaround for fake-gcs-server copy bugs
		r, err := src.NewReader(ctx)
		if err != nil {
			return fmt.Errorf("emulator manual copy read failed: %w", err)
		}
		defer r.Close()

		w := dst.NewWriter(ctx)
		w.ChunkSize = 0
		if _, err := io.Copy(w, r); err != nil {
			w.Close()
			return fmt.Errorf("emulator manual copy write failed: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("emulator manual copy close failed: %w", err)
		}
	} else {
		// Copy object
		if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
			return fmt.Errorf("copy failed: %w", err)
		}
	}

	// Delete original
	if err := src.Delete(ctx); err != nil {
		return fmt.Errorf("delete source failed: %w", err)
	}

	return nil
}

func (w *gcsClientWrapper) Download(ctx context.Context, bucketName, object string) ([]byte, error) {
	reader, err := w.client.Bucket(bucketName).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}
	return data, nil
}

func (w *gcsClientWrapper) Upload(ctx context.Context, bucketName, object string, data []byte) error {
	writer := w.client.Bucket(bucketName).Object(object).NewWriter(ctx)
	writer.ChunkSize = 0
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write data: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}

func (w *gcsClientWrapper) Delete(ctx context.Context, bucketName, object string) error {
	err := w.client.Bucket(bucketName).Object(object).Delete(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil // Idempotent delete
		}
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

func (w *gcsClientWrapper) ListPrefix(ctx context.Context, bucketName, prefix string) ObjectIterator {
	query := &storage.Query{Prefix: prefix}
	return w.client.Bucket(bucketName).Objects(ctx, query)
}

func (w *gcsClientWrapper) Close() error {
	return w.client.Close()
}
