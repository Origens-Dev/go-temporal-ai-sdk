package temporalai

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestAgentWorkflowEndsRootStreamOnce(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerDurableAgentTestActivities(env, func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		return &activities.InvokeModelResult{
			Content:      []activities.Part{{Type: "text", Text: "done"}},
			FinishReason: ai.FinishReason{Unified: ai.FinishStop},
		}, nil
	})
	var terminals []updates.StreamEndEvent
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.EndStreamArgs) error {
		terminals = append(terminals, args.Event)
		return nil
	}, activity.RegisterOptions{Name: activities.EndStreamActivity})

	env.ExecuteWorkflow(AgentWorkflow, AgentInput{
		AgentID: "agent-1", ModelID: "model", Prompt: "run",
		Stream: updates.Options{StreamID: "stream-1", ConversationID: "conversation-1"},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(terminals) != 1 || terminals[0].Outcome != updates.StreamOutcomeCompleted ||
		terminals[0].AgentID != "agent-1" || terminals[0].ConversationID != "conversation-1" {
		t.Fatalf("terminals = %#v", terminals)
	}
}

func TestAgentWorkflowEndsFailedRootStream(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerDurableAgentTestActivities(env, func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		return nil, temporal.NewNonRetryableApplicationError("model failed", "model-failed", nil)
	})
	var terminals []updates.StreamEndEvent
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.EndStreamArgs) error {
		terminals = append(terminals, args.Event)
		return nil
	}, activity.RegisterOptions{Name: activities.EndStreamActivity})

	env.ExecuteWorkflow(AgentWorkflow, AgentInput{ModelID: "model", Prompt: "run", Stream: updates.Options{StreamID: "stream-1"}})
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow unexpectedly succeeded")
	}
	if len(terminals) != 1 || terminals[0].Outcome != updates.StreamOutcomeFailed || terminals[0].Error == "" {
		t.Fatalf("terminals = %#v", terminals)
	}
}

func TestAgentWorkflowRetriesTerminalActivityWithoutRerunningAgent(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	modelCalls := 0
	registerDurableAgentTestActivities(env, func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		modelCalls++
		return &activities.InvokeModelResult{
			Content:      []activities.Part{{Type: "text", Text: "done"}},
			FinishReason: ai.FinishReason{Unified: ai.FinishStop},
		}, nil
	})
	terminalCalls := 0
	var emitted []updates.StreamEndEvent
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.EndStreamArgs) error {
		terminalCalls++
		if terminalCalls == 1 {
			return temporal.NewApplicationError("transient terminal failure", "transient")
		}
		emitted = append(emitted, args.Event)
		return nil
	}, activity.RegisterOptions{Name: activities.EndStreamActivity})

	env.ExecuteWorkflow(AgentWorkflow, AgentInput{ModelID: "model", Prompt: "run", Stream: updates.Options{StreamID: "stream-1"}})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if modelCalls != 1 || terminalCalls != 2 || len(emitted) != 1 || emitted[0].Outcome != updates.StreamOutcomeCompleted {
		t.Fatalf("modelCalls = %d, terminalCalls = %d, emitted = %#v", modelCalls, terminalCalls, emitted)
	}
}

func TestAgentWorkflowUsesDisconnectedContextToEndCanceledApproval(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerDurableAgentTestActivities(env, func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		return &activities.InvokeModelResult{
			Content:      []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "write"}},
			FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls},
		}, nil
	})
	var terminals []updates.StreamEndEvent
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.EndStreamArgs) error {
		terminals = append(terminals, args.Event)
		return nil
	}, activity.RegisterOptions{Name: activities.EndStreamActivity})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Millisecond)

	env.ExecuteWorkflow(AgentWorkflow, AgentInput{
		ModelID: "model",
		Prompt:  "run",
		Stream:  updates.Options{StreamID: "stream-1"},
		Tools:   []activities.ToolDefinition{{Name: "write", RequiresApproval: true}},
	})
	err := env.GetWorkflowError()
	if err == nil || !temporal.IsCanceledError(errors.Unwrap(err)) && !temporal.IsCanceledError(err) {
		t.Fatalf("workflow error = %T %v", err, err)
	}
	if len(terminals) != 1 || terminals[0].Outcome != updates.StreamOutcomeCanceled {
		t.Fatalf("terminals = %#v", terminals)
	}
}

func TestAgentWorkflowDoesNotEndSharedStreamForSubagentExecution(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerDurableAgentTestActivities(env, func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		return &activities.InvokeModelResult{
			Content:      []activities.Part{{Type: "text", Text: "child done"}},
			FinishReason: ai.FinishReason{Unified: ai.FinishStop},
		}, nil
	})
	terminalCalls := 0
	env.RegisterActivityWithOptions(func(context.Context, activities.EndStreamArgs) error {
		terminalCalls++
		return nil
	}, activity.RegisterOptions{Name: activities.EndStreamActivity})

	env.ExecuteWorkflow(AgentWorkflow, AgentInput{
		AgentID: "child", ModelID: "model", Prompt: "run", Stream: updates.Options{StreamID: "root-stream"},
		SubagentExecution: &SubagentExecutionContext{SubagentID: "child"},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if terminalCalls != 0 {
		t.Fatalf("subagent terminal calls = %d", terminalCalls)
	}
}

func registerDurableAgentTestActivities(env *testsuite.TestWorkflowEnvironment, model func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error)) {
	env.RegisterActivityWithOptions(model, activity.RegisterOptions{Name: activities.InvokeModelActivity})
	env.RegisterActivityWithOptions(func(context.Context, activities.WriteRecordArgs) error { return nil }, activity.RegisterOptions{Name: activities.WriteRecordActivity})
}
