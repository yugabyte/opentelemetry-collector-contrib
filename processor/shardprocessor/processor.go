package shardprocessor

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"

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

func (sp *processorImp) applyShardToNumberDataPoints(dps pmetric.NumberDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		sp.applyShardToAttributes(dps.At(i).Attributes())
	}
}

func (sp *processorImp) applyShardToHistogramDataPoints(dps pmetric.HistogramDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		sp.applyShardToAttributes(dps.At(i).Attributes())
	}
}

func (sp *processorImp) applyShardToSummaryDataPoints(dps pmetric.SummaryDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		sp.applyShardToAttributes(dps.At(i).Attributes())
	}
}

// Please see the below links for more details of implementation
// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// https://pkg.go.dev/go.opentelemetry.io/collector/pdata/pmetric#Metric
//
//
// MetricsData
// └─── ResourceMetrics
//   ├── Resource
//   ├── SchemaURL
//   └── ScopeMetrics
//      ├── Scope
//      ├── SchemaURL
//      └── Metric
//         ├── Name
//         ├── Description
//         ├── Unit
//         └── data
//            ├── Gauge
//            ├── Sum
//            ├── Histogram
//            ├── ExponentialHistogram
//            └── Summary

func (sp *processorImp) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		scopeMetrics := rm.ScopeMetrics()

		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			metrics := sm.Metrics()

			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					sp.applyShardToNumberDataPoints(m.Gauge().DataPoints())
				case pmetric.MetricTypeSum:
					sp.applyShardToNumberDataPoints(m.Sum().DataPoints())
				case pmetric.MetricTypeHistogram:
					sp.applyShardToHistogramDataPoints(m.Histogram().DataPoints())
				case pmetric.MetricTypeSummary:
					sp.applyShardToSummaryDataPoints(m.Summary().DataPoints())
				}

			}
		}
	}

	return sp.next.ConsumeMetrics(ctx, md)
}

func (sp *processorImp) applyShardToAttributes(attrs pcommon.Map) {
	shardID := 0

	// Priority = order of keys in the shard_labels config list
	for _, key := range sp.config.ShardLabels {
		if val, ok := attrs.Get(key); ok && val.Type() == pcommon.ValueTypeStr {
			s := val.Str()
			if s != "" {
				shardID = sp.calculateShardID(s)
				break
			}
		}
	}

	attrs.PutStr("shard_id", fmt.Sprintf("%d", shardID))
}

func (sp *processorImp) calculateShardID(name string) int {
	// Prometheus hashmod: https://github.com/prometheus/prometheus/blob/d344ea7bf4cc9e9e131a0318d10025982e9c4cc1/model/relabel/relabel.go#L290-L294
	sum := md5.Sum([]byte(name))
	last8 := binary.BigEndian.Uint64(sum[8:])
	return int(last8 % uint64(sp.config.NumShards))
}
