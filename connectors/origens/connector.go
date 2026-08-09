// Package origens combines a customer-owned durable update store with the
// Origens live/review stream. The platform publisher uses only a slot-bound
// Unix socket; customer workers never receive Valkey, S3, KMS, or catalog
// credentials.
package origens

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

const (
	DefaultSocketPath = "/run/gobeyond/host/host-report.sock"
	IngestPath        = "/v1/agent-review/events"
	Protocol          = "origens.agent-review/v1alpha1"
	defaultTimeout    = 2 * time.Second
	defaultMaxBytes   = 1024 * 1024
)

// DurableStore remains customer-owned (for example, a DynamoDB connector).
// Origens neither selects nor receives credentials for this store.
type DurableStore interface {
	updates.PreviewStore
	updates.RecordStore
}

type Options struct {
	Durable              DurableStore
	SocketPath           string
	Timeout              time.Duration
	MaxEventBytes        int
	OnPublicationFailure func(context.Context, updates.UpdateEvent, error)
}

// Connector implements updates.Connector. Durable operations commit first;
// broker publication failures after that commit are reported through the
// callback and never escape as retryable activity failures.
type Connector struct{ *updates.CompositeConnector }

func New(options Options) *Connector {
	publisher := NewPublisher(options.SocketPath, options.Timeout, options.MaxEventBytes)
	return &Connector{CompositeConnector: updates.NewCompositeConnector(updates.CompositeOptions{
		PreviewStore: options.Durable, RecordStore: options.Durable, LivePublisher: publisher,
		OnPublicationFailure: options.OnPublicationFailure,
	})}
}

// Publisher sends one validated protocol-v2 update to the local platform
// broker. Tenant/project/environment identity is bound to the socket mount by
// the host and is intentionally absent from request headers.
type Publisher struct {
	socketPath string
	maxBytes   int
	client     *http.Client
}

func NewPublisher(socketPath string, timeout time.Duration, maxBytes int) *Publisher {
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		socketPath = DefaultSocketPath
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		DisableCompression: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Publisher{socketPath: socketPath, maxBytes: maxBytes, client: &http.Client{Transport: transport, Timeout: timeout}}
}

func (p *Publisher) PublishUpdate(ctx context.Context, event updates.UpdateEvent) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("origens connector: publisher is not configured")
	}
	if err := updates.ValidateEvent(event); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("origens connector: encode event: %w", err)
	}
	if len(payload) > p.maxBytes {
		return fmt.Errorf("origens connector: event exceeds %d bytes", p.maxBytes)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://origens.internal"+IngestPath, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("origens connector: create ingest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Origens-Agent-Review-Protocol", Protocol)
	req.Header.Set("Idempotency-Key", event.EventBase().EventID)
	response, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("origens connector: publish over %s: %w", p.socketPath, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusNoContent {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("origens connector: broker status %d: %s", response.StatusCode, strings.TrimSpace(string(detail)))
	}
	return nil
}

var _ updates.Connector = (*Connector)(nil)
var _ updates.LivePublisher = (*Publisher)(nil)
