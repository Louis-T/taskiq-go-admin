package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s, err := Open(context.Background(), filepath.Join(t.TempDir(), "db", "test.db"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
