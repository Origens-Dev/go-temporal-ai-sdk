package activities

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

func TestInvokeModelDelegatesToProvider(t *testing.T) {
	model := ai.NewMockLanguageModel("model-1")
	model.GenerateFunc = func(_ context.Context, opts ai.LanguageModelCallOptions) (*ai.LanguageModelGenerateResult, error) {
		if got := ai.TextFromParts(opts.Prompt[0].Content); got != "hello" {
			t.Fatalf("prompt = %q", got)
		}
		return &ai.LanguageModelGenerateResult{Content: []ai.Part{ai.TextPart{Text: "hi"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
	}
	acts := New(Options{ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model-1": model}}})
	result, err := acts.InvokeModel(context.Background(), InvokeModelArgs{ModelID: "model-1", Options: LanguageModelCallOptionsFromAI(ai.LanguageModelCallOptions{Prompt: []ai.Message{ai.UserMessage("hello")}})})
	if err != nil {
		t.Fatal(err)
	}
	if got := ai.TextFromParts(result.ToAI().Content); got != "hi" {
		t.Fatalf("text = %q", got)
	}
}

func TestInvokeModelPublishesPreviewAndReturnsReceipt(t *testing.T) {
	model := ai.NewMockLanguageModel("model-1")
	model.GenerateFunc = func(_ context.Context, opts ai.LanguageModelCallOptions) (*ai.LanguageModelGenerateResult, error) {
		if _, leaked := opts.ProviderOptions[ProviderOptionsKey]; leaked {
			t.Fatalf("temporal provider option leaked: %#v", opts.ProviderOptions)
		}
		return &ai.LanguageModelGenerateResult{Content: []ai.Part{ai.TextPart{Text: "hello"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
	}
	connector := &recordingConnector{}
	acts := New(Options{
		ModelProvider:   ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model-1": model}},
		UpdateConnector: connector,
	})
	result, err := acts.InvokeModel(context.Background(), InvokeModelArgs{
		ModelID: "model-1",
		Options: LanguageModelCallOptionsFromAI(ai.LanguageModelCallOptions{ProviderOptions: ai.ProviderOptions{
			ProviderOptionsKey: updates.Options{Visible: true, StreamID: "stream-1", AttemptID: "attempt-1", TargetRecordID: "message:assistant-1"},
		}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(connector.begins) != 1 || connector.begins[0].Sequence != 0 {
		t.Fatalf("begins = %#v", connector.begins)
	}
	if len(connector.published) != 2 {
		t.Fatalf("published = %#v", connector.published)
	}
	chunk, ok := connector.published[0].(updates.PreviewChunkEvent)
	if !ok || chunk.Chunk["type"] != "text-delta" || chunk.Chunk["delta"] != "hello" {
		t.Fatalf("chunk = %#v", connector.published[0])
	}
	if len(connector.ends) != 1 || connector.ends[0].Outcome != updates.PreviewOutcomeSucceeded || connector.ends[0].Snapshot == nil || connector.ends[0].Snapshot.Text != "hello" {
		t.Fatalf("ends = %#v", connector.ends)
	}
	if len(result.PreviewReceipts) != 1 || !strings.HasPrefix(result.PreviewReceipts[0].AttemptID, "attempt-1:activity-") || !strings.HasSuffix(result.PreviewReceipts[0].AttemptID, ":text") || result.PreviewReceipts[0].TargetRecordID != "message:assistant-1" {
		t.Fatalf("receipts = %#v", result.PreviewReceipts)
	}
}

func TestPreviewFailurePolicyDefaultsStrict(t *testing.T) {
	modelCalled := false
	model := ai.NewMockLanguageModel("model-1")
	model.GenerateFunc = func(context.Context, ai.LanguageModelCallOptions) (*ai.LanguageModelGenerateResult, error) {
		modelCalled = true
		return &ai.LanguageModelGenerateResult{Content: []ai.Part{ai.TextPart{Text: "hi"}}}, nil
	}
	connector := &recordingConnector{beginErr: updates.NewStreamNotFoundError("stream-1")}
	acts := New(Options{ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model-1": model}}, UpdateConnector: connector})
	_, err := acts.InvokeModel(context.Background(), InvokeModelArgs{ModelID: "model-1", Options: LanguageModelCallOptionsFromAI(ai.LanguageModelCallOptions{ProviderOptions: ai.ProviderOptions{
		ProviderOptionsKey: updates.Options{Visible: true, StreamID: "stream-1"},
	}})})
	if !errors.Is(err, updates.ErrStreamNotFound) || modelCalled {
		t.Fatalf("err = %v, modelCalled = %v", err, modelCalled)
	}
}

func TestPreviewBestEffortOnlyDisablesMissingStream(t *testing.T) {
	model := ai.NewMockLanguageModel("model-1")
	model.GenerateFunc = func(context.Context, ai.LanguageModelCallOptions) (*ai.LanguageModelGenerateResult, error) {
		return &ai.LanguageModelGenerateResult{Content: []ai.Part{ai.TextPart{Text: "hi"}}}, nil
	}
	connector := &recordingConnector{beginErr: updates.NewStreamNotFoundError("stream-1")}
	acts := New(Options{ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model-1": model}}, UpdateConnector: connector})
	result, err := acts.InvokeModel(context.Background(), InvokeModelArgs{ModelID: "model-1", Options: LanguageModelCallOptionsFromAI(ai.LanguageModelCallOptions{ProviderOptions: ai.ProviderOptions{
		ProviderOptionsKey: updates.Options{Visible: true, StreamID: "stream-1", FailurePolicy: updates.FailurePolicyBestEffort},
	}})})
	if err != nil || ai.TextFromParts(result.ToAI().Content) != "hi" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
	if len(connector.published) != 0 || len(connector.ends) != 0 {
		t.Fatalf("connector was not disabled: %#v %#v", connector.published, connector.ends)
	}
}

func TestGenerateObjectUsesObjectLane(t *testing.T) {
	model := ai.NewMockLanguageModel("model-1")
	model.GenerateFunc = func(context.Context, ai.LanguageModelCallOptions) (*ai.LanguageModelGenerateResult, error) {
		return &ai.LanguageModelGenerateResult{Content: []ai.Part{ai.TextPart{Text: `{"name":"Ada"}`}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
	}
	connector := &recordingConnector{}
	acts := New(Options{ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model-1": model}}, UpdateConnector: connector})
	result, err := acts.GenerateObject(context.Background(), GenerateObjectArgs{ModelID: "model-1", Options: GenerateObjectOptionsFromAI(ai.GenerateObjectOptions{
		Prompt: "json", Schema: map[string]any{"type": "object"}, ProviderOptions: ai.ProviderOptions{ProviderOptionsKey: updates.Options{Visible: true, StreamID: "stream-1", TargetRecordID: "message:object-1"}},
	})})
	if err != nil {
		t.Fatal(err)
	}
	if result.Object.(map[string]any)["name"] != "Ada" || connector.begins[0].Lane != updates.LaneObject {
		t.Fatalf("result = %#v, begins = %#v", result, connector.begins)
	}
	if len(result.PreviewReceipts) != 1 || result.PreviewReceipts[0].Snapshot.Object.(map[string]any)["name"] != "Ada" {
		t.Fatalf("receipts = %#v", result.PreviewReceipts)
	}
}

func TestStreamObjectPublishesObjectAndElementPreviewManifests(t *testing.T) {
	model := ai.NewMockLanguageModel("model-1")
	model.StreamFunc = func(_ context.Context, opts ai.LanguageModelCallOptions) (*ai.LanguageModelStreamResult, error) {
		if _, leaked := opts.ProviderOptions[ProviderOptionsKey]; leaked {
			t.Fatalf("temporal provider option leaked: %#v", opts.ProviderOptions)
		}
		parts := make(chan ai.StreamPart, 3)
		parts <- ai.StreamPart{Type: "text-delta", TextDelta: `[{"name":"Ada"},`}
		parts <- ai.StreamPart{Type: "text-delta", TextDelta: `{"name":"Grace"}]`}
		parts <- ai.StreamPart{Type: "finish", FinishReason: ai.FinishReason{Unified: ai.FinishStop}}
		close(parts)
		return &ai.LanguageModelStreamResult{Stream: parts}, nil
	}
	connector := &recordingConnector{}
	acts := New(Options{
		ModelProvider:   ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model-1": model}},
		UpdateConnector: connector,
	})

	result, err := acts.StreamObject(context.Background(), StreamObjectArgs{
		ModelID: "model-1",
		Options: StreamObjectOptionsFromAI(ai.StreamObjectOptions{GenerateObjectOptions: ai.GenerateObjectOptions{
			Output: ai.OutputArray,
			Prompt: "return json",
			Schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"name": map[string]any{"type": "string"}},
				"required":   []any{"name"},
			},
			ProviderOptions: ai.ProviderOptions{ProviderOptionsKey: updates.Options{
				Visible: true, StreamID: "stream-1", AttemptID: "attempt-1", TargetRecordID: "message:object-1",
			}},
		}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Elements) != 2 || len(result.PreviewReceipts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(connector.begins) != 1 || connector.begins[0].Lane != updates.LaneObject {
		t.Fatalf("begins = %#v", connector.begins)
	}
	var objectChunk, elementChunk bool
	for _, event := range connector.published {
		chunk, ok := event.(updates.PreviewChunkEvent)
		if !ok {
			continue
		}
		if _, ok := chunk.Chunk["object"]; ok {
			objectChunk = true
		}
		if _, ok := chunk.Chunk["element"]; ok {
			elementChunk = true
		}
	}
	if !objectChunk || !elementChunk {
		t.Fatalf("published = %#v", connector.published)
	}
	finishChunks := 0
	for _, event := range connector.published {
		if chunk, ok := event.(updates.PreviewChunkEvent); ok && chunk.Chunk["type"] == "finish" {
			finishChunks++
		}
	}
	if finishChunks != 1 {
		t.Fatalf("finish chunks = %d, published = %#v", finishChunks, connector.published)
	}
	if len(connector.snapshots) == 0 || len(connector.ends) != 1 || connector.ends[0].Snapshot == nil {
		t.Fatalf("snapshots = %#v, ends = %#v", connector.snapshots, connector.ends)
	}
	if got := connector.ends[0].Snapshot.Elements; len(got) != 2 {
		t.Fatalf("completion elements = %#v", got)
	}
	if got := result.PreviewReceipts[0].Snapshot.Elements; len(got) != 2 {
		t.Fatalf("receipt elements = %#v", got)
	}
}

func TestInvokeEmbeddingModelDelegatesToProvider(t *testing.T) {
	model := ai.NewMockEmbeddingModel("embed-1")
	acts := New(Options{ModelProvider: ai.CustomProvider{EmbeddingModels: map[string]ai.EmbeddingModel{"embed-1": model}}})
	result, err := acts.InvokeEmbeddingModel(context.Background(), InvokeEmbeddingModelArgs{ModelID: "embed-1", Values: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Embeddings) != 2 {
		t.Fatalf("embeddings = %d", len(result.Embeddings))
	}
}

func TestInvokeModelStreamEndsFailedPreview(t *testing.T) {
	model := ai.NewMockLanguageModel("model-1")
	model.StreamFunc = func(context.Context, ai.LanguageModelCallOptions) (*ai.LanguageModelStreamResult, error) {
		parts := make(chan ai.StreamPart, 2)
		parts <- ai.StreamPart{Type: "text-delta", TextDelta: "partial"}
		parts <- ai.StreamPart{Type: "error", Err: errors.New("boom")}
		close(parts)
		return &ai.LanguageModelStreamResult{Stream: parts}, nil
	}
	connector := &recordingConnector{}
	acts := New(Options{ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model-1": model}}, UpdateConnector: connector})
	_, err := acts.InvokeModelStream(context.Background(), InvokeModelStreamArgs{ModelID: "model-1", Options: LanguageModelCallOptionsFromAI(ai.LanguageModelCallOptions{ProviderOptions: ai.ProviderOptions{
		ProviderOptionsKey: updates.Options{Visible: true, StreamID: "stream-1"},
	}})})
	if err == nil || len(connector.ends) != 1 || connector.ends[0].Outcome != updates.PreviewOutcomeFailed {
		t.Fatalf("err = %v, ends = %#v", err, connector.ends)
	}
}

func TestInvokeToolDoesNotWriteLifecycleRecords(t *testing.T) {
	connector := &recordingConnector{}
	acts := New(Options{
		UpdateConnector: connector,
		Tools:           map[string]ai.Tool{"lookup": {InputSchema: map[string]any{"type": "object"}, Execute: func(context.Context, ai.ToolCall, ai.ToolExecutionOptions) (any, error) { return "found", nil }}},
	})
	result, err := acts.InvokeTool(context.Background(), InvokeToolArgs{ToolCallID: "call-1", ToolName: "lookup", Input: map[string]any{}, Scope: updates.Scope{TaskID: "task-1"}})
	if err != nil || result.IsError || result.Output.Value != "found" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
	if len(connector.records) != 0 {
		t.Fatalf("tool activity wrote canonical records: %#v", connector.records)
	}
}

func TestInvokeToolPropagatesMetadataSandboxAndResultFiles(t *testing.T) {
	sandbox := testSandbox{}
	file := ai.ToolResultFile{URL: "https://example.test/report.pdf", MediaType: "application/pdf", Filename: "report.pdf"}
	var seen ai.ToolExecutionOptions
	acts := New(Options{Sandbox: sandbox, Tools: map[string]ai.Tool{"report": {
		InputSchema:  map[string]any{"type": "object"},
		ToolMetadata: ai.ProviderMetadata{"client": "mcp"},
		ToModelOutput: func(string, any, any) (ai.ToolResultOutput, error) {
			return ai.ToolResultOutput{Type: "json", Value: map[string]any{"ok": true}, Files: []ai.ToolResultFile{file}}, nil
		},
		Execute: func(_ context.Context, _ ai.ToolCall, options ai.ToolExecutionOptions) (any, error) {
			seen = options
			return map[string]any{"ok": true}, nil
		},
	}}})
	result, err := acts.InvokeTool(context.Background(), InvokeToolArgs{ToolCallID: "call-1", ToolName: "report", Input: map[string]any{}, ToolMetadata: ai.ProviderMetadata{"call": "model"}})
	if err != nil {
		t.Fatal(err)
	}
	if seen.Sandbox != sandbox || result.ToolMetadata["client"] != "mcp" || result.ToolMetadata["call"] != "model" || !reflect.DeepEqual(result.Output.Files, []ai.ToolResultFile{file}) {
		t.Fatalf("seen = %#v, result = %#v", seen, result)
	}
}

func TestInvokeToolApprovalPolicyStillGuardsExecution(t *testing.T) {
	executed := false
	acts := New(Options{Tools: map[string]ai.Tool{"write": {RequiresApproval: true, Execute: func(context.Context, ai.ToolCall, ai.ToolExecutionOptions) (any, error) {
		executed = true
		return "written", nil
	}}}})
	denied, err := acts.InvokeTool(context.Background(), InvokeToolArgs{ToolCallID: "call-1", ToolName: "write", Input: map[string]any{}})
	if err != nil || executed || denied.Output.Type != "execution-denied" {
		t.Fatalf("denied = %#v, executed = %v, err = %v", denied, executed, err)
	}
	approved := true
	allowed, err := acts.InvokeTool(context.Background(), InvokeToolArgs{ToolCallID: "call-1", ToolName: "write", Input: map[string]any{}, Approval: &ToolApprovalState{ApprovalID: "approval-1", Approved: &approved}})
	if err != nil || !executed || allowed.Output.Value != "written" {
		t.Fatalf("allowed = %#v, executed = %v, err = %v", allowed, executed, err)
	}
}

func TestInvokeToolRevalidatesDynamicApprovalPolicy(t *testing.T) {
	executed := false
	approved := true
	acts := New(Options{Tools: map[string]ai.Tool{"write": {
		NeedsApproval: func(context.Context, ai.ToolCall) (ai.ApprovalDecision, error) {
			return ai.Denied("policy changed"), nil
		},
		Execute: func(context.Context, ai.ToolCall, ai.ToolExecutionOptions) (any, error) {
			executed = true
			return "written", nil
		},
	}}})
	result, err := acts.InvokeTool(context.Background(), InvokeToolArgs{ToolCallID: "call-1", ToolName: "write", Input: map[string]any{}, Approval: &ToolApprovalState{Approved: &approved}})
	if err != nil || executed || result.Output.Type != "execution-denied" || result.Output.Reason != "policy changed" {
		t.Fatalf("result = %#v, executed = %v, err = %v", result, executed, err)
	}
}

func TestInvokeToolAddsScopeToExecutionContext(t *testing.T) {
	step := 2
	var seen map[string]any
	acts := New(Options{Tools: map[string]ai.Tool{"lookup": {InputSchema: map[string]any{"type": "object"}, Execute: func(_ context.Context, _ ai.ToolCall, opts ai.ToolExecutionOptions) (any, error) {
		seen = opts.Context.(map[string]any)
		return "ok", nil
	}}}})
	_, err := acts.InvokeTool(context.Background(), InvokeToolArgs{ToolCallID: "call-1", ToolName: "lookup", Input: map[string]any{}, Context: map[string]any{"messageId": "m1"}, Scope: updates.Scope{TaskID: "task-1", StepNumber: &step}})
	if err != nil {
		t.Fatal(err)
	}
	if seen["taskId"] != "task-1" || seen["stepNumber"] != step || seen["messageId"] != "m1" {
		t.Fatalf("context = %#v", seen)
	}
}

func TestInvokeToolArtifactsCompactLargeOutput(t *testing.T) {
	store := &recordingArtifactStore{}
	big := strings.Repeat("x", 60_000)
	acts := New(Options{ArtifactStore: store, Tools: map[string]ai.Tool{"lookup": {InputSchema: map[string]any{"type": "object"}, Execute: func(context.Context, ai.ToolCall, ai.ToolExecutionOptions) (any, error) { return big, nil }}}})
	result, err := acts.InvokeTool(context.Background(), InvokeToolArgs{
		ToolCallID: "call-1", ToolName: "lookup", Input: map[string]any{},
		Artifacts: &ToolArtifactPolicy{Enabled: true, WorkflowID: "workflow-1", RunID: "run-1", MaxInlineBytes: 1024, MaxPreviewBytes: 32},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.writes) != 2 {
		t.Fatalf("writes = %d", len(store.writes))
	}
	if _, ok := result.Result.(ToolArtifactValue); !ok {
		t.Fatalf("result = %#v", result.Result)
	}
}

func TestWriteRecordAndEndStreamValidateAndDelegate(t *testing.T) {
	connector := &recordingConnector{}
	acts := New(Options{UpdateConnector: connector})
	record := updates.WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: updates.RecordKindMessage, Status: "completed", Data: map[string]any{"text": "hi"}, UpdatedAt: 1}
	event := updates.NewRecordUpsertEvent("stream-1", record, "attempt-1", 1)
	if err := acts.WriteRecord(context.Background(), WriteRecordArgs{Event: event}); err != nil {
		t.Fatal(err)
	}
	terminal := updates.NewStreamEndEvent("stream-1", updates.StreamOutcomeCompleted, "", 2)
	if err := acts.EndStream(context.Background(), EndStreamArgs{Event: terminal}); err != nil {
		t.Fatal(err)
	}
	if len(connector.records) != 1 || connector.records[0].AcceptedAttemptID != "attempt-1" || len(connector.terminals) != 1 {
		t.Fatalf("records = %#v, terminals = %#v", connector.records, connector.terminals)
	}
	if err := acts.WriteRecord(context.Background(), WriteRecordArgs{}); !errors.Is(err, updates.ErrInvalidEvent) {
		t.Fatalf("invalid error = %v", err)
	}
}

func TestWirePreservesSignedToolApprovalParts(t *testing.T) {
	request := PartFromAI(ai.ToolApprovalRequestPart{ApprovalID: "approval-1", ToolCallID: "call-1", Signature: "signed", IsAutomatic: true})
	roundTrip, ok := request.ToAI().(ai.ToolApprovalRequestPart)
	if !ok || roundTrip.Signature != "signed" || !roundTrip.IsAutomatic {
		t.Fatalf("request = %#v", request.ToAI())
	}
	response := PartFromAI(ai.ToolApprovalResponsePart{ApprovalID: "approval-1", Approved: true, ProviderMetadata: ai.ProviderMetadata{"provider": "mock"}})
	responseAI, ok := response.ToAI().(ai.ToolApprovalResponsePart)
	if !ok || !responseAI.Approved || !reflect.DeepEqual(responseAI.ProviderMetadata, ai.ProviderMetadata{"provider": "mock"}) {
		t.Fatalf("response = %#v", response.ToAI())
	}
}

func TestInvokeModelStreamCompactsToolInputAndReasoningFile(t *testing.T) {
	model := ai.NewMockLanguageModel("stream-1")
	model.StreamFunc = func(context.Context, ai.LanguageModelCallOptions) (*ai.LanguageModelStreamResult, error) {
		parts := make(chan ai.StreamPart, 3)
		parts <- ai.StreamPart{Type: "tool-call", ToolCallID: "call-1", ToolName: "extract", ToolInput: `{"uri":"s3://bucket/file"}`}
		parts <- ai.StreamPart{Type: "reasoning-file", Content: ai.ReasoningFilePart{Data: ai.FileData{Type: "url", URL: "https://example.test/reasoning.png"}, MediaType: "image/png"}}
		parts <- ai.StreamPart{Type: "finish", FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls}}
		close(parts)
		return &ai.LanguageModelStreamResult{Stream: parts}, nil
	}
	acts := New(Options{ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"stream-1": model}}})
	result, err := acts.InvokeModelStream(context.Background(), InvokeModelStreamArgs{ModelID: "stream-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Result == nil || len(result.Result.Content) != 2 {
		t.Fatalf("result = %#v", result)
	}
	input, ok := result.Result.Content[0].Input.(map[string]any)
	if !ok || input["uri"] != "s3://bucket/file" || result.Result.Content[1].Type != "reasoning-file" {
		t.Fatalf("content = %#v", result.Result.Content)
	}
}

func TestWirePreservesProviderMetadataAndPerformance(t *testing.T) {
	metadata := ai.ProviderMetadata{"googleVertex": map[string]any{"thoughtSignature": "sig-1"}}
	text := PartFromAI(ai.TextPart{Text: "hello", ProviderMetadata: metadata})
	textAI, ok := text.ToAI().(ai.TextPart)
	if !ok || !reflect.DeepEqual(textAI.ProviderMetadata, metadata) {
		t.Fatalf("text = %#v", text.ToAI())
	}
	performance := ai.StepPerformance{StepTime: 2 * time.Second, TimeToFirstOutputToken: 25 * time.Millisecond}
	stream := StreamPartFromAI(ai.StreamPart{Type: "finish-step", Performance: performance, ToolMetadata: ai.ProviderMetadata{"client": "mcp"}}).ToAI()
	if stream.Performance.StepTime != performance.StepTime || stream.ToolMetadata["client"] != "mcp" {
		t.Fatalf("stream = %#v", stream)
	}
}

func TestInvokeModelStreamRejectsEmptyProviderStream(t *testing.T) {
	model := ai.NewMockLanguageModel("stream-1")
	model.StreamFunc = func(context.Context, ai.LanguageModelCallOptions) (*ai.LanguageModelStreamResult, error) {
		parts := make(chan ai.StreamPart)
		close(parts)
		return &ai.LanguageModelStreamResult{Stream: parts}, nil
	}
	acts := New(Options{ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"stream-1": model}}})
	_, err := acts.InvokeModelStream(context.Background(), InvokeModelStreamArgs{ModelID: "stream-1"})
	if !ai.IsNoOutputGeneratedError(err) {
		t.Fatalf("err = %T %v", err, err)
	}
}

func TestToolDefinitionsFromAISortsMapKeys(t *testing.T) {
	definitions := ToolDefinitionsFromAI(map[string]ai.Tool{"zebra": {}, "alpha": {}, "middle": {}})
	got := []string{definitions[0].Name, definitions[1].Name, definitions[2].Name}
	if !reflect.DeepEqual(got, []string{"alpha", "middle", "zebra"}) {
		t.Fatalf("order = %#v", got)
	}
}

type recordingConnector struct {
	begins     []updates.PreviewBeginEvent
	snapshots  []updates.PreviewSnapshotEvent
	ends       []updates.PreviewEndEvent
	records    []updates.RecordUpsertEvent
	terminals  []updates.StreamEndEvent
	published  []updates.UpdateEvent
	beginErr   error
	publishErr error
}

func (c *recordingConnector) BeginPreview(_ context.Context, event updates.PreviewBeginEvent) error {
	c.begins = append(c.begins, event)
	return c.beginErr
}
func (c *recordingConnector) CheckpointPreview(_ context.Context, event updates.PreviewSnapshotEvent) error {
	c.snapshots = append(c.snapshots, event)
	return nil
}
func (c *recordingConnector) EndPreview(_ context.Context, event updates.PreviewEndEvent) error {
	c.ends = append(c.ends, event)
	return nil
}
func (c *recordingConnector) UpsertRecord(_ context.Context, event updates.RecordUpsertEvent) error {
	c.records = append(c.records, event)
	return nil
}
func (c *recordingConnector) EndStream(_ context.Context, event updates.StreamEndEvent) error {
	c.terminals = append(c.terminals, event)
	return nil
}
func (c *recordingConnector) PublishUpdate(_ context.Context, event updates.UpdateEvent) error {
	c.published = append(c.published, event)
	return c.publishErr
}

type recordingArtifactStore struct{ writes []ToolArtifactWriteInput }

func (s *recordingArtifactStore) PutToolArtifact(_ context.Context, input ToolArtifactWriteInput) (*ToolArtifactRef, error) {
	s.writes = append(s.writes, input)
	return &ToolArtifactRef{ArtifactID: fmt.Sprintf("%s/%s/%s.json", input.WorkflowID, input.ToolCallID, input.Kind), Kind: input.Kind, OriginalBytes: input.OriginalBytes, ContentType: input.ContentType, SHA256: input.SHA256}, nil
}

type testSandbox struct{}

func (testSandbox) RunCommand(context.Context, ai.SandboxCommand) (ai.SandboxCommandResult, error) {
	return ai.SandboxCommandResult{Stdout: "ok"}, nil
}
func (testSandbox) ReadFile(context.Context, string) ([]byte, error) { return []byte("ok"), nil }
func (testSandbox) WriteFile(context.Context, string, []byte) error  { return nil }
