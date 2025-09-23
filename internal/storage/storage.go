package storage

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Storage defines the interface for any backup storage backend.
type Storage interface {
	Write(ctx context.Context, obj *unstructured.Unstructured, hash string) (changed bool, err error)
	Read(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, string, error)
	ReadAllHashes(ctx context.Context, gvk schema.GroupVersionKind) (map[string]string, error)
	Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
	MarkTombstone(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
	ListTombstones(ctx context.Context) ([]TombstoneEntry, error)
	TombstonePath(gvk schema.GroupVersionKind, namespace, name string) string
	DeleteTombstone(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
}

type TombstoneEntry struct {
	GVK       schema.GroupVersionKind
	Namespace string
	Name      string
	ModTime   time.Time
}
