package temporalai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestRunAgentCompactsToolArtifactsBeforeNextTool(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	store := &regressionArtifactStore{}
	big := strings.Repeat("x", 600_000)
	var modelCalls int
	var secondPromptBytes int
	acts := activities.New(activities.Options{
		ArtifactStore: store,
		Tools: map[string]ai.Tool{
			"big_lookup": {Execute: func(context.Context, ai.ToolCall, ai.ToolExecutionOptions) (any, error) { return big, nil }},
			"small_lookup": {Execute: func(_ context.Context, _ ai.ToolCall, opts ai.ToolExecutionOptions) (any, error) {
				payload, _ := json.Marshal(opts.Messages)
				if len(payload) > 80_000 {
					return nil, fmt.Errorf("tool messages too large: %d bytes", len(payload))
				}
				return "small result", nil
			}},
		},
	})
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		modelCalls++
		switch modelCalls {
		case 1:
			return &activities.InvokeModelResult{Content: []activities.Part{{Type: "tool-call", ToolCallID: "call-big", ToolName: "big_lookup"}}, FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls}}, nil
		case 2:
			payload, _ := json.Marshal(args.Options.Prompt)
			secondPromptBytes = len(payload)
			if strings.Contains(string(payload), strings.Repeat("x", 2_000)) || !strings.Contains(string(payload), "artifactRef") {
				t.Fatalf("second prompt was not compacted: %.500s", payload)
			}
			return &activities.InvokeModelResult{Content: []activities.Part{{Type: "tool-call", ToolCallID: "call-small", ToolName: "small_lookup"}}, FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls}}, nil
		default:
			return &activities.InvokeModelResult{Content: []activities.Part{{Type: "text", Text: "done"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
		}
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})
	env.RegisterActivityWithOptions(acts.InvokeTool, activity.RegisterOptions{Name: activities.InvokeToolActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		AgentID: "agent-1", ModelID: "model-1", Prompt: "run lookups", ToolExecution: ToolExecutionSequential,
		ToolArtifacts: activities.ToolArtifactPolicy{Enabled: true, MaxInlineBytes: 1_024, MaxPreviewBytes: 64},
		Tools:         []activities.ToolDefinition{{Name: "big_lookup"}, {Name: "small_lookup"}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(store.writes) < 2 || secondPromptBytes == 0 || secondPromptBytes > 80_000 {
		t.Fatalf("writes=%d secondPromptBytes=%d", len(store.writes), secondPromptBytes)
	}
}

func TestRunAgentApprovalAllowsOrDeniesToolExecution(t *testing.T) {
	for _, test := range []struct {
		name      string
		approved  bool
		wantCalls int
	}{
		{name: "allowed", approved: true, wantCalls: 1},
		{name: "denied", approved: false, wantCalls: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			var toolCalls int
			var toolApproval *activities.ToolApprovalState
			env.RegisterActivityWithOptions(func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
				return &activities.InvokeModelResult{Content: []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "write"}}, FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls}}, nil
			}, activity.RegisterOptions{Name: activities.InvokeModelActivity})
			env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeToolArgs) (*activities.InvokeToolResult, error) {
				toolCalls++
				toolApproval = args.Approval
				return &activities.InvokeToolResult{ToolCallID: args.ToolCallID, ToolName: args.ToolName, Output: ai.ToolResultOutput{Type: "text", Value: "written"}}, nil
			}, activity.RegisterOptions{Name: activities.InvokeToolActivity})
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(ToolApprovalResponseSignalName("call-1:approval"), ToolApprovalResponse{ApprovalID: "call-1:approval", Approved: test.approved, Reason: "decision"})
			}, time.Millisecond)

			env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
				ModelID: "model-1", Prompt: "write", MaxSteps: 1,
				Tools: []activities.ToolDefinition{{Name: "write", RequiresApproval: true}},
			})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if toolCalls != test.wantCalls {
				t.Fatalf("tool calls = %d", toolCalls)
			}
			var result AgentResult
			if err := env.GetWorkflowResult(&result); err != nil {
				t.Fatal(err)
			}
			if test.approved {
				if toolApproval == nil || toolApproval.Approved == nil || !*toolApproval.Approved {
					t.Fatalf("approval = %#v", toolApproval)
				}
			} else if got := result.Steps[0].ToolResults[0].Output; got.Type != "execution-denied" || got.Reason != "decision" {
				t.Fatalf("denied result = %#v", got)
			}
		})
	}
}

func TestRunAgentFallsBackAfterLocalToolTimeout(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var localStarts, activityStarts, executions int
	env.SetOnLocalActivityStartedListener(func(info *activity.Info, _ context.Context, _ []interface{}) {
		if info.ActivityType.Name == activities.InvokeToolActivity {
			localStarts++
		}
	})
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		if info.ActivityType.Name == activities.InvokeToolActivity {
			activityStarts++
		}
	})
	env.RegisterActivityWithOptions(func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		return &activities.InvokeModelResult{Content: []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "lookup"}}, FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeToolArgs) (*activities.InvokeToolResult, error) {
		executions++
		if executions == 1 {
			return nil, temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)
		}
		return &activities.InvokeToolResult{ToolCallID: args.ToolCallID, ToolName: args.ToolName, Output: ai.ToolResultOutput{Type: "text", Value: "ok"}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeToolActivity})

	env.ExecuteWorkflow(testAgentWorkflowWithOneLocalToolAttemptRegression, AgentInput{
		ModelID: "model-1", Prompt: "lookup", MaxSteps: 1,
		DefaultToolBoundary: activities.ToolExecutionBoundaryLocalActivity,
		Tools:               []activities.ToolDefinition{{Name: "lookup"}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if localStarts != 1 || activityStarts != 1 || executions != 2 {
		t.Fatalf("local=%d activity=%d executions=%d", localStarts, activityStarts, executions)
	}
}

func TestRunAgentSubagentCanBeInspectedMessagedAndWaited(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(regressionInspectableSubagentChild, workflow.RegisterOptions{Name: "regressionInspectableSubagentChild"})
	var modelCalls int
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		modelCalls++
		switch modelCalls {
		case 1:
			return regressionToolCall("call-research", "research", map[string]any{"task": "inspect the repository"}), nil
		case 2:
			return regressionToolCall("call-inspect", InspectSubagentToolName, map[string]any{"subagentId": "research-call-research"}), nil
		case 3:
			var snapshot SubagentSnapshot
			regressionDecodeLastToolResult(t, args, &snapshot)
			if snapshot.Status != SubagentStatusRunning {
				t.Fatalf("inspect = %#v", snapshot)
			}
			return regressionToolCall("call-message", MessageSubagentToolName, map[string]any{"subagentId": "research-call-research", "message": "focus on workflow boundaries"}), nil
		case 4:
			return regressionToolCall("call-wait", WaitSubagentToolName, map[string]any{"subagentId": "research-call-research", "timeoutSeconds": 5}), nil
		default:
			var waited SubagentWaitResult
			regressionDecodeLastToolResult(t, args, &waited)
			if waited.Snapshot.Status != SubagentStatusCompleted || waited.Snapshot.Text != "focus on workflow boundaries" {
				t.Fatalf("wait = %#v", waited)
			}
			return &activities.InvokeModelResult{Content: []activities.Part{{Type: "text", Text: "research complete"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
		}
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		AgentID: "parent", ModelID: "model-1", Prompt: "delegate", ToolExecution: ToolExecutionSequential,
		Subagents: []SubagentDefinition{{
			Tool:         activities.ToolDefinition{Name: "research"},
			Agent:        &AgentInput{AgentID: "researcher", ModelID: "child-model"},
			WorkflowType: "regressionInspectableSubagentChild",
		}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result AgentResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.Text != "research complete" || modelCalls != 5 {
		t.Fatalf("result=%#v modelCalls=%d", result, modelCalls)
	}
}

func TestRunAgentCanCancelBackgroundSubagent(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(regressionInspectableSubagentChild, workflow.RegisterOptions{Name: "regressionCancelableSubagentChild"})
	var modelCalls int
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		modelCalls++
		switch modelCalls {
		case 1:
			return regressionToolCall("call-research", "research", map[string]any{"task": "wait"}), nil
		case 2:
			return regressionToolCall("call-cancel", CancelSubagentToolName, map[string]any{"subagentId": "research-call-research", "reason": "no longer needed"}), nil
		case 3:
			return regressionToolCall("call-wait", WaitSubagentToolName, map[string]any{"subagentId": "research-call-research", "timeoutSeconds": 5}), nil
		default:
			var waited SubagentWaitResult
			regressionDecodeLastToolResult(t, args, &waited)
			if waited.Snapshot.Status != SubagentStatusCanceled {
				t.Fatalf("wait = %#v", waited)
			}
			return &activities.InvokeModelResult{Content: []activities.Part{{Type: "text", Text: "canceled"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
		}
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		AgentID: "parent", ModelID: "model-1", Prompt: "delegate then cancel", ToolExecution: ToolExecutionSequential,
		Subagents: []SubagentDefinition{{
			Tool:         activities.ToolDefinition{Name: "research"},
			Agent:        &AgentInput{AgentID: "researcher", ModelID: "child-model"},
			WorkflowType: "regressionCancelableSubagentChild",
		}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if modelCalls != 4 {
		t.Fatalf("model calls = %d", modelCalls)
	}
}

func regressionToolCall(callID, toolName string, input any) *activities.InvokeModelResult {
	return &activities.InvokeModelResult{
		Content:      []activities.Part{{Type: "tool-call", ToolCallID: callID, ToolName: toolName, Input: input}},
		FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls},
	}
}

func regressionDecodeLastToolResult(t *testing.T, args activities.InvokeModelArgs, target any) {
	t.Helper()
	last := args.Options.Prompt[len(args.Options.Prompt)-1]
	if last.Role != ai.RoleTool || len(last.Content) != 1 {
		t.Fatalf("last prompt = %#v", last)
	}
	payload, err := json.Marshal(last.Content[0].Output.Value)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, target); err != nil {
		t.Fatal(err)
	}
}

func regressionInspectableSubagentChild(ctx workflow.Context, input AgentInput) (*AgentResult, error) {
	if err := publishSubagentProgress(ctx, input, SubagentSnapshot{Status: SubagentStatusRunning, Sequence: 3, StepType: "research", Text: "working"}, false); err != nil {
		return nil, err
	}
	var message SubagentMessage
	received := false
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(workflow.GetSignalChannel(ctx, SubagentMessageSignalName), func(channel workflow.ReceiveChannel, _ bool) {
		channel.Receive(ctx, &message)
		received = true
	})
	selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
	selector.Select(ctx)
	if !received {
		return nil, ctx.Err()
	}
	if err := publishSubagentProgress(ctx, input, SubagentSnapshot{Status: SubagentStatusCompleted, Sequence: 4, StepNumber: 1, StepType: "summary", Text: message.Content, FinishReason: ai.FinishStop}, false); err != nil {
		return nil, err
	}
	return &AgentResult{AgentID: input.AgentID, ModelID: input.ModelID, Text: message.Content, FinishReason: ai.FinishStop}, nil
}

func testAgentWorkflowWithOneLocalToolAttemptRegression(ctx workflow.Context, input AgentInput) (*AgentResult, error) {
	return RunAgent(ctx, input, ActivityOptions{LocalTool: workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}})
}

type regressionArtifactStore struct {
	writes []activities.ToolArtifactWriteInput
}

func (s *regressionArtifactStore) PutToolArtifact(_ context.Context, input activities.ToolArtifactWriteInput) (*activities.ToolArtifactRef, error) {
	s.writes = append(s.writes, input)
	return &activities.ToolArtifactRef{
		ArtifactID: fmt.Sprintf("%s/%s/%s/%s.json", input.WorkflowID, input.ToolCallID, input.Kind, input.SHA256),
		Kind:       input.Kind, OriginalBytes: input.OriginalBytes, ContentType: input.ContentType, SHA256: input.SHA256,
	}, nil
}
