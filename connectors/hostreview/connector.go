// Package hostreview exposes the generic hosted-runtime review connector.
//
// The compatibility package at connectors/origens remains available, while
// public frameworks can depend on this product-neutral import path.
package hostreview

import (
	"time"

	origensconnector "github.com/Origens-Dev/go-temporal-ai-sdk/connectors/origens"
)

const (
	DefaultSocketPath = origensconnector.DefaultSocketPath
	IngestPath        = origensconnector.IngestPath
	Protocol          = origensconnector.Protocol
)

type DurableStore = origensconnector.DurableStore
type Options = origensconnector.Options
type Connector = origensconnector.Connector
type Publisher = origensconnector.Publisher

func New(options Options) *Connector {
	return origensconnector.New(options)
}

func NewPublisher(socketPath string, timeout time.Duration, maxEventBytes int) *Publisher {
	return origensconnector.NewPublisher(socketPath, timeout, maxEventBytes)
}
