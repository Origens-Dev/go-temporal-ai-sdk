# Origens hosted connector

`connectors/origens` combines two deliberately separate responsibilities:

- a durable preview and record store supplied and credentialed by the customer;
- the hosted Origens live/review publisher, reached only through the worker
  slot's platform-owned Unix socket.

The durable commit is authoritative. After it succeeds, a broker publication
failure is reported through `OnPublicationFailure` and is not returned as a
retryable activity error. This prevents a stream outage from rerunning a model,
tool side effect, or already-committed customer write.

```go
connector := origens.New(origens.Options{
    Durable: customerDynamoConnector,
    OnPublicationFailure: func(ctx context.Context, event updates.UpdateEvent, err error) {
        logger.Error("agent review publication gap", "event_id", event.EventBase().EventID, "error", err)
    },
})

acts := activities.New(activities.Options{
    RuntimeResolver: runtimeResolver,
    UpdateConnector: connector,
})
```

The default socket is `/run/gobeyond/host/host-report.sock`. Hosted workers should
not override it. Local development can provide `SocketPath` explicitly and run
an HTTP handler on that Unix socket.

The publisher sends protocol-v2 update events to
`POST /v1/agent-review/events` with protocol and idempotency headers. It does
not send organization, project, environment, billing, retention, or storage
identity. The host derives those values from the immutable worker-slot binding.

The connector never needs Valkey, S3, KMS, catalog, or platform database
credentials. Those remain behind the hosted broker.
