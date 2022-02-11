package tableindexbench

import (
	"context"
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	Version = "0.36.1"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func setupDolt() {
	ctx := context.Background()
	urlString := "file://"
	ddb, err := doltdb.LoadDoltDBWithParams(ctx, types.Format_Default, urlString, filesys.LocalFS, map[string]interface{}{})
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, Version)
}

func BenchmarkDiskTableIndex(b *testing.B) {
	// TODO: Add some parameter to load a local persistent chunk store with a table file index and a way to either walk the tree or generate random
	// hash lookups.
}
