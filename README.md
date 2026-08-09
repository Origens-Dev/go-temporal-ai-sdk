# go-temporal-ai-sdk

Temporal-native activities and workflow helpers for
[`github.com/Origens-Dev/go-ai`](https://github.com/Origens-Dev/go-ai).

`go-ai` owns provider-compatible model and tool behavior. This module adds the
Temporal-specific attempt, retry, acceptance, persistence, and replay boundary.

## Packages

- `activities`: worker-side model, object, embedding, tool, record, and stream
  termination activities.
- `temporalai`: deterministic workflow helpers and the durable agent loop.
- `updates`: protocol-v2 preview, record, replay, connector, and relay types.
- `connectors/appsync-dynamodb`: AppSync Events live delivery with DynamoDB
  preview/record/replay storage.
- `connectors/redis-dynamodb`: Redis Pub/Sub or Streams live delivery with the
  same DynamoDB storage model.
- `connectors/origens`: customer-owned durable storage plus hosted Origens live
  and review delivery over a slot-bound Unix socket.

The language-neutral frozen contract and fixtures live in [`protocol/v2`](protocol/v2/README.md).
See [`docs/streaming.md`](docs/streaming.md) for runtime semantics and examples,
[`docs/origens-connector.md`](docs/origens-connector.md) for the hosted connector,
and [`docs/migration-v2.md`](docs/migration-v2.md) for the v0.3 to v0.4 API map.

## Provenance

This Origens module was forked from
[`holbrookab/go-temporal-ai-sdk` `v0.4.0`](https://github.com/holbrookab/go-temporal-ai-sdk/releases/tag/v0.4.0)
at commit `5836767`. The protocol-v2 schemas, event names, activity names,
signal/query names, and Temporal `GetVersion` change IDs remain compatible with
that source release.

## Worker registration

```go
connector := appsyncdynamodb.New(appsyncdynamodb.Options{
    AWSConfig:         cfg,
    TableName:         "chat-production",
    AppSyncHTTPDomain: "example.appsync-api.us-west-2.amazonaws.com",
})

acts := activities.New(activities.Options{
    ModelProvider:   provider,
    UpdateConnector: connector,
    Sandbox:         sandbox,
    Tools: map[string]ai.Tool{
        "lookup": lookupTool,
    },
})
temporalai.RegisterActivities(worker, acts)
temporalai.RegisterAgentWorkflow(worker)
```

Workers with compiled per-agent dependencies can supply a resolver. The
resolver must return the exact requested agent and compiled revision; a mismatch
fails as the non-retryable Temporal application error type
`go-temporal-ai-sdk.RuntimeMismatch` before provider or tool code executes.

```go
acts := activities.New(activities.Options{
    RuntimeResolver: activities.AgentRuntimeResolverFunc(
        func(ctx context.Context, scope activities.RuntimeScope) (activities.AgentRuntime, error) {
            compiled, err := registry.Resolve(ctx, scope.AgentID, scope.CompiledRevision)
            if err != nil {
                return activities.AgentRuntime{}, err
            }
            return activities.AgentRuntime{
                AgentID:          scope.AgentID,
                CompiledRevision: compiled.Revision,
                ModelProvider:    compiled.Provider,
                Tools:            compiled.Tools,
                Sandbox:          compiled.Sandbox,
            }, nil
        },
    ),
})
```

An empty `AgentID` deliberately bypasses the resolver and uses the original
`ModelProvider`, `Tools`, `ArtifactStore`, and `Sandbox` options. This preserves
existing direct helper and pre-agent payload behavior.

`UpdateConnector` is strict by default. A preview storage or live publication
failure fails the model activity and can cause Temporal to retry the provider
call. `updates.FailurePolicyBestEffort` suppresses only a typed missing-stream
error; it does not suppress auth, throttling, or transport failures.

## Live preview followed by durable acceptance

```go
options := ai.LanguageModelCallOptions{
    Prompt: []ai.Message{ai.UserMessage("Summarize this")},
    ProviderOptions: ai.ProviderOptions{
        activities.ProviderOptionsKey: updates.Options{
            Visible:        true,
            StreamID:       workflow.GetInfo(ctx).WorkflowExecution.ID,
            TargetRecordID: "message:assistant-1",
            Lane:           updates.LaneText,
        },
    },
}

previewed, err := temporalai.InvokeModelStream(ctx, "model-id", options)
if err != nil {
    return err
}

receipt := previewed.PreviewReceipts[0]
record := updates.WorkflowRecord{
    RecordID:      receipt.TargetRecordID,
    RecordVersion: 1,
    Kind:          updates.RecordKindMessage,
    Status:        "completed",
    Data: map[string]any{
        "role": "assistant",
        "text": receipt.Snapshot.Text,
    },
    Scope: receipt.Scope,
}
if err := temporalai.WriteRecord(ctx, streamID, record, receipt.AttemptID); err != nil {
    return err
}
```

The model activity emits `preview-begin`, `preview-chunk`, periodic
`preview-snapshot`, and `preview-end`. A successful preview remains provisional.
Only the separate workflow-scheduled `WriteRecord` activity emits the canonical
`record-upsert` that names the exact accepted attempt.

When every accepted record is readable, close the subscription explicitly:

```go
if err := temporalai.EndStream(ctx, streamID, updates.StreamOutcomeCompleted, ""); err != nil {
    return err
}
```

## Durable agents

`temporalai.AgentWorkflow` is the stable registered root entry point under
`go-temporal-ai-sdk.AgentWorkflow`; `temporalai.RunAgent` remains the embeddable
loop for custom workflows. The root wrapper owns stream termination and emits
exactly one protocol-v2 `stream-end` after accepted records are written. It uses
a disconnected workflow context when the run is canceled, and child subagents
never close the root stream.

The agent loop uses the same boundary automatically. Model activities
return preview receipts. The workflow writes canonical message, tool, and
tool-approval interaction records in separate record activities, so retrying
record persistence cannot rerun a model or side-effecting tool.

The new record commands are behind Temporal `GetVersion` change
`go-temporal-ai-sdk.durable-records-v2`. Replaying a history created before
v0.4 takes `workflow.DefaultVersion` and schedules none of the new activities;
new workflow runs record version `1` and use the v2 path.

```go
result, err := temporalai.RunAgent(ctx, temporalai.AgentInput{
    AgentID:      "researcher",
    CompiledRevision: "sha256:compiled-agent-revision",
    ModelID:      "model-id",
    Instructions: "Use tools when useful.",
    Prompt:       "Find the latest durable execution notes.",
    Tools:        activities.ToolDefinitionsFromAI(tools),
    Stream: updates.Options{
        Visible:  true,
        StreamID: workflow.GetInfo(ctx).WorkflowExecution.ID,
    },
})
```

Tool approval is a generic `interaction` record with
`interactionType: "tool-approval"`. The Go workflow authors the question and
choices and waits for the existing Temporal signal. Signed provider approval
fields remain preserved in the model wire types, but do not create a human gate
unless the workflow requests one.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
