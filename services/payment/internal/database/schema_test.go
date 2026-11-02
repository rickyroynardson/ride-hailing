package database

import (
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestSchemaVersionIsLatestMigration(t *testing.T) {
	entries, err := os.ReadDir("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	var versions []int64
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if v, err := strconv.ParseInt(prefix, 10, 64); ok && err == nil && strings.HasSuffix(e.Name(), ".sql") {
			versions = append(versions, v)
		}
	}
	if len(versions) == 0 {
		t.Fatal("no migrations found")
	}
	if latest := slices.Max(versions); latest != SchemaVersion {
		t.Fatalf("SchemaVersion is %d but the newest migration is %d; bump SchemaVersion", SchemaVersion, latest)
	}
}
