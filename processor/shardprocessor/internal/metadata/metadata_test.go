package metadata

import (
	"testing"
)

func TestTypeValue(t *testing.T) {
	if Type.String() != "shardprocessor" {
		t.Fatalf("expected Type.String() = shardprocessor, got %s", Type.String())
	}
}
