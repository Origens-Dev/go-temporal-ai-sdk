# Changelog

## Unreleased

## 0.1.0-alpha.5 - 2026-08-09

- Carry the durable conversation/session ID independently from the execution
  stream ID on hosted review events, allowing one conversation to contain
  multiple reviewable runs without overloading either identity.

## 0.1.0-alpha.4 - 2026-08-09

- Carry the compiler-owned agent ID on every hosted review event, including
  terminal stream events, so the platform can validate it against the agent
  definitions bound to the worker deployment.

## 0.1.0-alpha.3 - 2026-08-09

- Use the existing slot-private host-report Unix socket for hosted agent-review
  publication, avoiding a second sandbox mount while preserving the same
  identity and credential boundary.

## 0.1.0-alpha.2 - 2026-08-09

- Added fully resolved per-tool Temporal task queues and an agent-level model
  execution boundary, including local model and object-stream activities for
  realtime durable agents.
- Added heartbeat defaults and worker-side heartbeat emission for long-running
  remote model, embedding, object, stream, and tool activities.
- Added `updates.CompositeConnector`, which commits customer-owned durable
  preview and record state before best-effort live publication without turning
  a post-commit stream failure into a retry of provider or tool work.
- Added `connectors/origens`, combining a customer-provided durable store with
  the hosted Origens agent-review publisher over a slot-bound Unix socket. The
  publisher carries no customer identity or Valkey, S3, KMS, or catalog
  credentials.

## 0.1.0-alpha.1 - 2026-08-09

- Forked from `holbrookab/go-temporal-ai-sdk` `v0.4.0` at commit `5836767`
  and moved the module and `go-ai` imports to the `Origens-Dev` organization.
- Added agent-scoped worker runtime resolution keyed by `AgentID` and compiled
  revision across language model, object, embedding, stream, and tool
  activities. Resolver identity drift is a typed non-retryable Temporal error;
  empty `AgentID` inputs retain the original static worker runtime.
- Added the stable `go-temporal-ai-sdk.AgentWorkflow` root workflow wrapper.
  It emits one completed, failed, or canceled protocol-v2 `stream-end` from a
  disconnected context and prevents child subagents from closing the root
  stream.
- Made approval waits cancellation-aware and persist canceled interaction state
  from a disconnected workflow context.
- Updated the Go-compatible Temporal dependencies to SDK `v1.45.0` and API
  `v1.62.12` without changing existing activity, signal, query, or `GetVersion`
  names.
- Added runtime mismatch, workflow terminal retry, cancellation, root/subagent
  ownership, and compatibility coverage plus CI and release automation.

## 0.4.0 - 2026-07-10

- Replaced the v1 `streaming` package with the clean-break protocol-v2
  `updates` package and frozen cross-language JSON schemas/fixtures.
- Added provisional `preview-begin`, `preview-chunk`, `preview-snapshot`, and
  `preview-end` events plus activity-returned preview receipts.
- Added workflow-authored, versioned `message`, `tool`, `interaction`, `task`,
  and `subagent` records. `acceptedAttemptId` supersedes only the exact provider
  attempt selected by the workflow.
- Added separate idempotent `WriteRecord` and `EndStream` activities and
  workflow helpers so persistence/fanout retries cannot rerun models or tools.
- Guarded the new `RunAgent` record commands with Temporal `GetVersion` so
  workflow histories started before v0.4 replay without nondeterminism.
- Replaced tool lifecycle events and synthesized checkpoint output with generic
  tool records and `tool-approval` interaction records authored by workflows.
- Updated AppSync/DynamoDB and Redis/DynamoDB adapters to persist preview
  manifests, monotonic current records, reusable store cursors, and terminal
  stream state while publishing the common v2 envelope. Event identity hashes
  include the full semantic payload, including exact attempt acceptance.
- Preserved signed provider approval fields in Temporal wire serialization.
- Added the streaming/failure guide, complete v0.3 migration table, adapter
  guides, conformance fixtures, and execution-count coverage.

## 0.3.0 - 2026-07-10

- Bumped `go-ai` to `v0.3.0` and preserved its signed tool-approval request and
  approval-response parts across Temporal message and stream serialization.
- Rejected provider streams that close without generated output, matching the
  new `go-ai` stream behavior.
- Made map-to-tool-definition conversion deterministic and revalidated dynamic
  approval policy immediately before executing an already approved tool.
- Added inspectable asynchronous subagents backed by Temporal child workflows,
  including durable progress snapshots and built-in list, inspect, wait,
  message, and cancel tools for the parent agent.

## 0.2.19 - 2026-06-01

- Added typed `streaming.ErrStreamNotFound` handling and
  `StreamFailurePolicyBestEffort` so visible streaming can degrade to no-op for
  missing stream rows without swallowing other connector errors.

## 0.2.18 - 2026-06-01

- Added connector-backed visible attempt updates for non-streaming
  `InvokeModel` and `GenerateObject` calls when temporal stream options are
  provided.
- Added `StreamObject` activity registration and the `temporalai.StreamObject`
  workflow helper for object-native streaming calls.

## 0.2.17 - 2026-06-01

- Added `GenerateObject` activity registration and the `temporalai.GenerateObject`
  workflow helper for durable structured-output calls.

## 0.2.16 - 2026-06-01

- Bumped `go-ai` to `v0.2.7` so SDK consumers inherit the OpenRouter
  structured-output schema fix.

## 0.2.15 - 2026-05-31

- Bumped `go-ai` to `v0.2.6`.
- Preserved streamed `reasoning-file` parts when compacting model stream
  results for workflow history.
- Preserved text/file provider metadata across the Temporal wire format for
  Vertex thought-signature replay.

## Historical Notes

- Added tool execution boundaries for durable agents. Tools still default to
  regular Temporal activities, but agents can opt into local tool activities by
  default or override the boundary per tool.
- Added `temporalai.ActivityOptions.LocalTool` and
  `temporalai.InvokeToolLocal` for configuring and invoking local tool
  activities.
- Added configurable local-tool timeout fallback. Local tool timeouts default to
  retrying the same tool call as a regular activity; agents can set
  `LocalToolTimeoutFallbackNone` to surface the local timeout instead.
- Added local language/embedding model invocation options for short routing or
  classification calls.
- Added default Temporal activity summaries for model, stream, embedding, tool,
  and lifecycle invoke helpers.
- Documented the regular-vs-local activity tradeoff for short idempotent tools
  versus slower or more durable tool work.

## 0.2.0 - 2026-05-01

- Set the `go-ai` dependency to `v0.2.0` for release publishing.
- Removed the local sibling `replace` directive from `go.mod`.
- Added Apache-2.0 licensing and README license guidance.

## 0.1.0

- Initial Temporal-native runtime for `go-ai` model calls, tool calls, agents, and visible streams.
