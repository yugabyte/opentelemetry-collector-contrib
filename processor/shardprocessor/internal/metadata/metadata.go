package metadata

import "go.opentelemetry.io/collector/component"

// Type is the identifier for the shard processor
var Type = component.MustNewType("shardprocessor")
