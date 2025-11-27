package shardprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func newProcessor(shards int, labels []string) *processorImp {
	return &processorImp{
		config: &Config{
			NumShards:   shards,
			ShardLabels: labels,
		},
	}
}

func TestApplyShardToAttributes(t *testing.T) {
	tests := []struct {
		name       string
		labels     map[string]string
		wantShard0 bool
	}{
		{
			name: "primary label takes priority",
			labels: map[string]string{
				"universe_uuid": "11111111-2222-3333-4444-555555555555",
				"job":           "should_not_use",
			},
			wantShard0: false,
		},
		{
			name: "fallback to job when universe_uuid missing",
			labels: map[string]string{
				"job": "platform-yugaware-ui",
			},
			wantShard0: false,
		},
		{
			name:       "default shard when no labels match",
			labels:     map[string]string{},
			wantShard0: true,
		},
	}

	p := newProcessor(4, []string{"universe_uuid", "job"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			for k, v := range tt.labels {
				attrs.PutStr(k, v)
			}

			p.applyShardToAttributes(attrs)

			shardVal, ok := attrs.Get("shard_id")
			if !ok {
				t.Fatalf("expected shard_id attribute to exist")
			}

			if tt.wantShard0 && shardVal.Str() != "0" {
				t.Fatalf("expected shard_id=0, got %s", shardVal.Str())
			}

			if !tt.wantShard0 && shardVal.Str() == "0" {
				t.Fatalf("did not expect shard_id=0, got 0")
			}
		})
	}
}

func TestConsumeMetrics_ShardsAppliedToDatapoints(t *testing.T) {
	next := new(consumertest.MetricsSink)
	p, _ := New(&Config{
		NumShards:   4,
		ShardLabels: []string{"universe_uuid"},
	}, next)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("universe_uuid", "cluster-123")

	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("key", "value")

	err := p.ConsumeMetrics(context.Background(), md)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := next.AllMetrics()[0]
	outDp := out.ResourceMetrics().At(0).
		ScopeMetrics().At(0).
		Metrics().At(0).
		Gauge().DataPoints().At(0)

	shardVal, ok := outDp.Attributes().Get("shard_id")
	if !ok || shardVal.Str() == "" {
		t.Fatalf("expected shard_id to be added to datapoint")
	}
}

func TestCalculateShardID_Distribution(t *testing.T) {
	p := newProcessor(4, []string{"universe_uuid"})

	seen := map[int]bool{}
	for _, v := range []string{"a", "b", "c", "d", "e", "f"} {
		seen[p.calculateShardID(v)] = true
	}

	if len(seen) < 2 {
		t.Fatalf("expected shard distribution across multiple shards, got %v", seen)
	}
}

func BenchmarkApplyShardToAttributes(b *testing.B) {
	p := newProcessor(16, []string{"universe_uuid", "job", "node"})
	attrs := pcommon.NewMap()
	attrs.PutStr("universe_uuid", "cluster-123")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.applyShardToAttributes(attrs)
	}
}

func TestCalculateShardID_KnownUniverseUUIDs(t *testing.T) {
	p := newProcessor(8, nil)

	tests := []struct {
		uuid     string
		expected int
	}{
		{"0acc4e79-4dc7-4f05-b7d5-9b01f31b5aef", 1},
		{"beac01ad-18e2-499b-b542-192dadce0f02", 2},
		{"a46b68ca-413f-488b-bb51-8bd2a054f285", 4},
		{"ed4c6b65-1c82-4024-944b-9fb6e7cb1959", 6},
		{"c380ccbe-b613-4aef-a5e9-b03a243c5816", 5},
	}

	for _, tt := range tests {
		t.Run(tt.uuid, func(t *testing.T) {
			if got := p.calculateShardID(tt.uuid); got != tt.expected {
				t.Fatalf("expected shard %d, got %d", tt.expected, got)
			}
		})
	}
}
