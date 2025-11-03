package shardprocessor

import (
	"context"
	"fmt"
	"hash/fnv"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
)

type processorImp struct {
	config *Config
	next   consumer.Metrics
}

func New(cfg *Config, next consumer.Metrics) (processor.Metrics, error) {
	return &processorImp{
		config: cfg,
		next:   next,
	}, nil
}

func (sp *processorImp) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (sp *processorImp) Shutdown(_ context.Context) error {
	return nil
}

func (sp *processorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (sp *processorImp) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		attrs := rm.Resource().Attributes()

		labelKey := sp.config.ShardLabel
		labelValue, exists := attrs.Get(labelKey)

		var shardID int

		if exists && labelValue.Type() == pcommon.ValueTypeStr {
			strVal := labelValue.Str()
			if strVal != "" {
				shardID = sp.calculateShardID(strVal)
			} else {
				shardID = 0
			}
		} else {
			shardID = 0
		}

		attrs.PutStr("shard_id", fmt.Sprintf("%d", shardID))
	}

	return sp.next.ConsumeMetrics(ctx, md)
}

func (sp *processorImp) calculateShardID(name string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(name))
	return int(hasher.Sum32() % uint32(sp.config.NumShards))
}
