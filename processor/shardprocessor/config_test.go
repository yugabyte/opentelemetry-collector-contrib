package shardprocessor

import (
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/shardprocessor/internal/metadata"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
)

func TestLoadConfig(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	if err != nil {
		t.Fatalf("failed creating nop factories: %v", err)
	}

	factories.Processors[metadata.Type] = NewFactory()

	cfg, err := otelcoltest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)
	if err != nil {
		t.Fatalf("config.yaml validation failed: %v", err)
	}

	p := cfg.Processors[component.NewID(metadata.Type)].(*Config)

	if p.NumShards != 6 {
		t.Fatalf("expected num_shards=6, got %d", p.NumShards)
	}
	if len(p.ShardLabels) != 2 {
		t.Fatalf("expected 2 shard_labels, got %d", len(p.ShardLabels))
	}
}
