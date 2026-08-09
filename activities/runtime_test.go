package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"go.temporal.io/sdk/temporal"
)

func TestAgentRuntimeResolverScopesEveryExecutableActivity(t *testing.T) {
	want := RuntimeScope{AgentID: "researcher", CompiledRevision: "sha256:abc"}
	resolverErr := errors.New("resolver stopped activity")
	var scopes []RuntimeScope
	acts := New(Options{RuntimeResolver: AgentRuntimeResolverFunc(func(_ context.Context, scope RuntimeScope) (AgentRuntime, error) {
		scopes = append(scopes, scope)
		return AgentRuntime{}, resolverErr
	})})

	calls := []func() error{
		func() error {
			_, err := acts.InvokeModel(context.Background(), InvokeModelArgs{AgentID: want.AgentID, CompiledRevision: want.CompiledRevision, ModelID: "model"})
			return err
		},
		func() error {
			_, err := acts.GenerateObject(context.Background(), GenerateObjectArgs{AgentID: want.AgentID, CompiledRevision: want.CompiledRevision, ModelID: "model"})
			return err
		},
		func() error {
			_, err := acts.StreamObject(context.Background(), StreamObjectArgs{AgentID: want.AgentID, CompiledRevision: want.CompiledRevision, ModelID: "model"})
			return err
		},
		func() error {
			_, err := acts.InvokeModelStream(context.Background(), InvokeModelStreamArgs{AgentID: want.AgentID, CompiledRevision: want.CompiledRevision, ModelID: "model"})
			return err
		},
		func() error {
			_, err := acts.InvokeEmbeddingModel(context.Background(), InvokeEmbeddingModelArgs{AgentID: want.AgentID, CompiledRevision: want.CompiledRevision, ModelID: "embedding"})
			return err
		},
		func() error {
			_, err := acts.InvokeTool(context.Background(), InvokeToolArgs{AgentID: want.AgentID, CompiledRevision: want.CompiledRevision, ToolCallID: "call-1", ToolName: "lookup"})
			return err
		},
	}
	for index, call := range calls {
		if err := call(); !errors.Is(err, resolverErr) {
			t.Fatalf("call %d error = %v", index, err)
		}
	}
	if len(scopes) != len(calls) {
		t.Fatalf("resolver calls = %d, want %d", len(scopes), len(calls))
	}
	for index, scope := range scopes {
		if scope != want {
			t.Fatalf("scope %d = %#v, want %#v", index, scope, want)
		}
	}
}

func TestAgentRuntimeRevisionMismatchIsTypedAndNonRetryable(t *testing.T) {
	acts := New(Options{RuntimeResolver: AgentRuntimeResolverFunc(func(context.Context, RuntimeScope) (AgentRuntime, error) {
		return AgentRuntime{AgentID: "researcher", CompiledRevision: "sha256:old"}, nil
	})})
	_, err := acts.InvokeModel(context.Background(), InvokeModelArgs{
		AgentID: "researcher", CompiledRevision: "sha256:new", ModelID: "model",
	})
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("error = %T %v", err, err)
	}
	if applicationErr.Type() != RuntimeMismatchErrorType || !applicationErr.NonRetryable() {
		t.Fatalf("type = %q, nonretryable = %v", applicationErr.Type(), applicationErr.NonRetryable())
	}
	var details RuntimeMismatchError
	if detailErr := applicationErr.Details(&details); detailErr != nil {
		t.Fatal(detailErr)
	}
	if details.RequestedCompiledRevision != "sha256:new" || details.ResolvedCompiledRevision != "sha256:old" {
		t.Fatalf("details = %#v", details)
	}
}

func TestEmptyAgentIDUsesLegacyDefaultRuntime(t *testing.T) {
	resolverCalled := false
	model := ai.NewMockLanguageModel("model")
	model.GenerateFunc = func(context.Context, ai.LanguageModelCallOptions) (*ai.LanguageModelGenerateResult, error) {
		return &ai.LanguageModelGenerateResult{Content: []ai.Part{ai.TextPart{Text: "legacy"}}}, nil
	}
	acts := New(Options{
		ModelProvider: ai.CustomProvider{LanguageModels: map[string]ai.LanguageModel{"model": model}},
		RuntimeResolver: AgentRuntimeResolverFunc(func(context.Context, RuntimeScope) (AgentRuntime, error) {
			resolverCalled = true
			return AgentRuntime{}, nil
		}),
	})
	result, err := acts.InvokeModel(context.Background(), InvokeModelArgs{ModelID: "model"})
	if err != nil {
		t.Fatal(err)
	}
	if resolverCalled || ai.TextFromParts(result.ToAI().Content) != "legacy" {
		t.Fatalf("resolverCalled = %v, result = %#v", resolverCalled, result)
	}
}
