package activities

import (
	"context"
	"errors"
	"fmt"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"go.temporal.io/sdk/temporal"
)

const RuntimeMismatchErrorType = "go-temporal-ai-sdk.RuntimeMismatch"

// RuntimeScope selects the compiled agent runtime used by an activity. Empty
// values preserve the original single-runtime behavior.
type RuntimeScope struct {
	AgentID          string `json:"agentId,omitempty"`
	CompiledRevision string `json:"compiledRevision,omitempty"`
}

// AgentRuntime contains the worker-side dependencies compiled for one agent.
// AgentID and CompiledRevision are returned by the resolver and verified before
// any provider or tool code is invoked.
type AgentRuntime struct {
	AgentID          string
	CompiledRevision string
	ModelProvider    ai.Provider
	Tools            map[string]ai.Tool
	ArtifactStore    ToolArtifactStore
	Sandbox          ai.Sandbox
}

type AgentRuntimeResolver interface {
	ResolveAgentRuntime(context.Context, RuntimeScope) (AgentRuntime, error)
}

type AgentRuntimeResolverFunc func(context.Context, RuntimeScope) (AgentRuntime, error)

func (f AgentRuntimeResolverFunc) ResolveAgentRuntime(ctx context.Context, scope RuntimeScope) (AgentRuntime, error) {
	return f(ctx, scope)
}

// RuntimeMismatchError is returned when a resolver supplies a runtime for a
// different agent or compiled revision. Activities translate it to a typed,
// non-retryable Temporal application error.
type RuntimeMismatchError struct {
	RequestedAgentID          string `json:"requestedAgentId,omitempty"`
	RequestedCompiledRevision string `json:"requestedCompiledRevision,omitempty"`
	ResolvedAgentID           string `json:"resolvedAgentId,omitempty"`
	ResolvedCompiledRevision  string `json:"resolvedCompiledRevision,omitempty"`
}

func (e *RuntimeMismatchError) Error() string {
	return fmt.Sprintf(
		"agent runtime mismatch: requested agent %q revision %q, resolved agent %q revision %q",
		e.RequestedAgentID,
		e.RequestedCompiledRevision,
		e.ResolvedAgentID,
		e.ResolvedCompiledRevision,
	)
}

func (a *Activities) resolveRuntime(ctx context.Context, scope RuntimeScope) (AgentRuntime, error) {
	legacy := AgentRuntime{}
	if a == nil {
		return legacy, nil
	}
	legacy = AgentRuntime{
		AgentID:          scope.AgentID,
		CompiledRevision: scope.CompiledRevision,
		ModelProvider:    a.provider,
		Tools:            a.tools,
		ArtifactStore:    a.artifacts,
		Sandbox:          a.sandbox,
	}
	// Empty AgentID is the compatibility path for workers configured before
	// agent-scoped runtime resolution existed.
	if a.runtimeResolver == nil || scope.AgentID == "" {
		return legacy, nil
	}
	runtime, err := a.runtimeResolver.ResolveAgentRuntime(ctx, scope)
	if err != nil {
		return AgentRuntime{}, err
	}
	if runtime.AgentID != scope.AgentID ||
		(scope.CompiledRevision != "" && runtime.CompiledRevision != scope.CompiledRevision) {
		mismatch := &RuntimeMismatchError{
			RequestedAgentID:          scope.AgentID,
			RequestedCompiledRevision: scope.CompiledRevision,
			ResolvedAgentID:           runtime.AgentID,
			ResolvedCompiledRevision:  runtime.CompiledRevision,
		}
		return AgentRuntime{}, temporal.NewNonRetryableApplicationError(
			mismatch.Error(),
			RuntimeMismatchErrorType,
			nil,
			*mismatch,
		)
	}
	return runtime, nil
}

func (r AgentRuntime) languageModel(modelID string) (ai.LanguageModel, error) {
	if r.ModelProvider == nil {
		return nil, errors.New("model provider is required")
	}
	model := r.ModelProvider.LanguageModel(modelID)
	if model == nil {
		return nil, fmt.Errorf("language model %q not found", modelID)
	}
	return model, nil
}

func runtimeScope(agentID, compiledRevision string) RuntimeScope {
	return RuntimeScope{AgentID: agentID, CompiledRevision: compiledRevision}
}
