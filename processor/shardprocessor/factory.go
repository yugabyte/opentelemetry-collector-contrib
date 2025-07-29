package shardprocessor

import (
	"context"
	"errors"

	"github.com/yugabyte/opentelemetry-collector-contrib/processor/shardprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelStable),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		NumShards: 2,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	settings processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	config, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("invalid configuration for shardprocessor")
	}
	return New(config, next)
}
