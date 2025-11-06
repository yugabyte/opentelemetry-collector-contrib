package shardprocessor

import "go.opentelemetry.io/collector/component"

type Config struct {
	component.Config

	NumShards   int      `mapstructure:"num_shards"`
	ShardLabels []string `mapstructure:"shard_labels"`
}
