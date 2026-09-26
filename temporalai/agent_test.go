package temporalai

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestToolRecordMetadataFromContextCopiesTaskFields(t *testing.T) {
	metadata := toolLifecycleMetadataFromContext(struct {
		TaskID             string `json:"taskId"`
		TaskTitle          string `json:"taskTitle"`
		SkillName          string `json:"skillName"`
		AssistantMessageID string `json:"assistantMessageId"`
	}{TaskID: "task-1", TaskTitle: "Find records", SkillName: "Search", AssistantMessageID: "message-1"})
	if metadata["taskId"] != "task-1" || metadata["taskTitle"] != "Find records" || metadata["skillName"] != "Search" {
		t.Fatalf("metadata = %#v", metadata)
	}
	if _, leaked := metadata["assistantMessageId"]; leaked {
		t.Fatalf("metadata leaked persistence field: %#v", metadata)
	}
}

func TestAgentStepScopeIncludesTaskSkillAndStep(t *testing.T) {
	scope := agentStepScope(AgentInput{
		AgentID: "agent-1",
		Stream:  updates.Options{Scope: updates.Scope{DisplayMode: updates.DisplayModeTask}},
		ToolContext: struct {
			TaskID    string `json:"taskId"`
			TaskTitle string `json:"taskTitle"`
			SkillName string `json:"skillName"`
		}{TaskID: "task-1", TaskTitle: "Find records", SkillName: "Search"},
	}, "step-0", 0, "initial")
	if scope.DisplayMode != updates.DisplayModeTask || scope.AgentID != "agent-1" || scope.TaskID != "task-1" || scope.TaskTitle != "Find records" || scope.SkillName != "Search" {
		t.Fatalf("scope = %#v", scope)
	}
	if scope.StepID != "step-0" || scope.StepNumber == nil || *scope.StepNumber != 0 || scope.StepType != "initial" {
		t.Fatalf("step scope = %#v", scope)
	}
}

func TestRunAgentExecutesToolActivityAndContinues(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var modelCalls int
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		modelCalls++
		if args.AgentID != "agent-1" || args.CompiledRevision != "sha256:agent-1" {
			t.Fatalf("model runtime scope = %#v", args)
		}
		if modelCalls == 1 {
			return &activities.InvokeModelResult{Content: []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "lookup", Input: map[string]any{"query": "temporal"}}}, FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls}}, nil
		}
		last := args.Options.Prompt[len(args.Options.Prompt)-1]
		if last.Role != ai.RoleTool {
			t.Fatalf("last prompt = %#v", last)
		}
		return &activities.InvokeModelResult{Content: []activities.Part{{Type: "text", Text: "Temporal result"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeToolArgs) (*activities.InvokeToolResult, error) {
		if args.AgentID != "agent-1" || args.CompiledRevision != "sha256:agent-1" || args.Scope.StepID != "step-0" || args.ToolName != "lookup" {
			t.Fatalf("tool args = %#v", args)
		}
		return &activities.InvokeToolResult{ToolCallID: args.ToolCallID, ToolName: args.ToolName, Input: args.Input, Output: ai.ToolResultOutput{Type: "text", Value: "lookup output"}, Result: "lookup output"}, nil
	}, activity.RegisterOptions{Name: activities.InvokeToolActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		AgentID: "agent-1", CompiledRevision: "sha256:agent-1", ModelID: "model-1", Prompt: "run lookup", ToolExecution: ToolExecutionSequential,
		Tools: []activities.ToolDefinition{{Name: "lookup", InputSchema: map[string]any{"type": "object"}}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result AgentResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.Text != "Temporal result" || len(result.Steps) != 2 || modelCalls != 2 {
		t.Fatalf("result = %#v, calls = %d", result, modelCalls)
	}
}

func TestRunAgentAppliesFirstToolChoiceOnlyOnFirstStep(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var choices []ai.ToolChoice
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		choices = append(choices, args.Options.ToolChoice)
		if len(choices) == 1 {
			return &activities.InvokeModelResult{
				Content:      []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "extractDocument"}},
				FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls},
			}, nil
		}
		return &activities.InvokeModelResult{Content: []activities.Part{{Type: "text", Text: "done"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeToolArgs) (*activities.InvokeToolResult, error) {
		return &activities.InvokeToolResult{ToolCallID: args.ToolCallID, ToolName: args.ToolName, Output: ai.ToolResultOutput{Type: "json", Value: map[string]any{"ok": true}}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeToolActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		ModelID: "model-1", Prompt: "open document", FirstToolChoice: ai.ToolChoiceFor("extractDocument"),
		Tools: []activities.ToolDefinition{{Name: "extractDocument", InputSchema: map[string]any{"type": "object"}}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(choices) != 2 || choices[0].Type != "tool" || choices[0].ToolName != "extractDocument" || choices[1].Type != "auto" {
		t.Fatalf("tool choices = %#v", choices)
	}
}

func TestToolExecutionBoundaryPrecedenceAndTimeoutFallbackDefault(t *testing.T) {
	input := AgentInput{
		DefaultToolBoundary: activities.ToolExecutionBoundaryLocalActivity,
		Tools: []activities.ToolDefinition{
			{Name: "inherits"},
			{Name: "override", ExecutionBoundary: activities.ToolExecutionBoundaryActivity},
		},
	}
	if got := toolExecutionBoundary(input, "inherits"); got != activities.ToolExecutionBoundaryLocalActivity {
		t.Fatalf("inherited boundary = %q", got)
	}
	if got := toolExecutionBoundary(input, "override"); got != activities.ToolExecutionBoundaryActivity {
		t.Fatalf("override boundary = %q", got)
	}
	if got := localToolTimeoutFallback(input); got != LocalToolTimeoutFallbackActivity {
		t.Fatalf("timeout fallback = %q", got)
	}
	input.LocalToolTimeoutFallback = LocalToolTimeoutFallbackNone
	if got := localToolTimeoutFallback(input); got != LocalToolTimeoutFallbackNone {
		t.Fatalf("explicit timeout fallback = %q", got)
	}
}

func TestAgentActivityOptionsApplyModelBoundaryAndToolQueue(t *testing.T) {
	input := AgentInput{
		DefaultModelBoundary: activities.ToolExecutionBoundaryLocalActivity,
		Tools: []activities.ToolDefinition{
			{Name: "lookup", TaskQueue: "support-tools__prod"},
		},
	}
	options := agentActivityOptions(input)[0]
	if options.LanguageModelBoundary != activities.ToolExecutionBoundaryLocalActivity {
		t.Fatalf("model boundary = %q", options.LanguageModelBoundary)
	}
	if got := agentToolTaskQueue(input, "lookup"); got != "support-tools__prod" {
		t.Fatalf("tool queue = %q", got)
	}
	if got := agentToolTaskQueue(input, "missing"); got != "" {
		t.Fatalf("missing tool queue = %q", got)
	}
}

func TestRunAgentWritesCanonicalMessageAndToolRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var modelCalls int
	var records []updates.RecordUpsertEvent
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeModelStreamArgs) (*activities.InvokeModelStreamResult, error) {
		modelCalls++
		if modelCalls == 1 {
			return &activities.InvokeModelStreamResult{Result: &activities.LanguageModelGenerateResult{
				Content:      []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "lookup", Input: map[string]any{"query": "temporal"}}},
				FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls},
			}, PreviewReceipts: []updates.PreviewReceipt{{AttemptID: "attempt-tool", TargetRecordID: "tool:call-1", Lane: updates.LaneToolInput, Outcome: updates.PreviewOutcomeSucceeded, Snapshot: updates.Snapshot{Text: `{"query":"temporal"}`}}}}, nil
		}
		return &activities.InvokeModelStreamResult{Result: &activities.LanguageModelGenerateResult{Content: []activities.Part{{Type: "text", Text: "done"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, PreviewReceipts: []updates.PreviewReceipt{{AttemptID: "attempt-text", TargetRecordID: "message:stream-1:step-1", Lane: updates.LaneText, Outcome: updates.PreviewOutcomeSucceeded, Snapshot: updates.Snapshot{Text: "done"}}}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeModelStreamActivity})
	env.RegisterActivityWithOptions(func(context.Context, activities.InvokeToolArgs) (*activities.InvokeToolResult, error) {
		return &activities.InvokeToolResult{ToolCallID: "call-1", ToolName: "lookup", Output: ai.ToolResultOutput{Type: "text", Value: "ok"}, Result: "ok"}, nil
	}, activity.RegisterOptions{Name: activities.InvokeToolActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.WriteRecordArgs) error {
		records = append(records, args.Event)
		return nil
	}, activity.RegisterOptions{Name: activities.WriteRecordActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		AgentID: "agent-1", ModelID: "model-1", Prompt: "run", ToolExecution: ToolExecutionSequential,
		Stream: updates.Options{Visible: true, StreamID: "stream-1"},
		Tools:  []activities.ToolDefinition{{Name: "lookup", InputSchema: map[string]any{"type": "object"}}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var sawAcceptedTool, sawTerminalTool, sawAcceptedMessage bool
	for _, event := range records {
		switch {
		case event.Record.RecordID == "tool:call-1" && event.Record.RecordVersion == 1:
			sawAcceptedTool = event.AcceptedAttemptID == "attempt-tool" && event.Record.Status == "running"
		case event.Record.RecordID == "tool:call-1" && event.Record.RecordVersion == 2:
			sawTerminalTool = event.Record.Status == "succeeded"
		case event.Record.RecordID == "message:stream-1:step-1":
			sawAcceptedMessage = event.AcceptedAttemptID == "attempt-text" && event.Record.Data["text"] == "done"
		}
	}
	if !sawAcceptedTool || !sawTerminalTool || !sawAcceptedMessage {
		t.Fatalf("records = %#v", records)
	}
}

func TestRunAgentScopesCallerStreamIdentityBasesPerStep(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var modelCalls int
	var streamOptions []updates.Options
	var records []updates.RecordUpsertEvent
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeModelStreamArgs) (*activities.InvokeModelStreamResult, error) {
		modelCalls++
		payload, err := json.Marshal(args.Options.ProviderOptions[activities.ProviderOptionsKey])
		if err != nil {
			t.Fatal(err)
		}
		var options updates.Options
		if err := json.Unmarshal(payload, &options); err != nil {
			t.Fatal(err)
		}
		streamOptions = append(streamOptions, options)
		if modelCalls == 1 {
			return &activities.InvokeModelStreamResult{Result: &activities.LanguageModelGenerateResult{
				Content:      []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "lookup"}},
				FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls},
			}}, nil
		}
		return &activities.InvokeModelStreamResult{Result: &activities.LanguageModelGenerateResult{
			Content:      []activities.Part{{Type: "text", Text: "done"}},
			FinishReason: ai.FinishReason{Unified: ai.FinishStop},
		}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeModelStreamActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.InvokeToolArgs) (*activities.InvokeToolResult, error) {
		return &activities.InvokeToolResult{ToolCallID: args.ToolCallID, ToolName: args.ToolName, Output: ai.ToolResultOutput{Type: "text", Value: "ok"}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeToolActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.WriteRecordArgs) error {
		records = append(records, args.Event)
		return nil
	}, activity.RegisterOptions{Name: activities.WriteRecordActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		AgentID: "agent-1", ModelID: "model-1", Prompt: "run", ToolExecution: ToolExecutionSequential,
		Stream: updates.Options{
			Visible: true, StreamID: "stream-1", AttemptID: "turn-42", TargetRecordID: "message:assistant-1",
		},
		Tools: []activities.ToolDefinition{{Name: "lookup", InputSchema: map[string]any{"type": "object"}}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(streamOptions) != 2 {
		t.Fatalf("stream options = %#v", streamOptions)
	}
	if streamOptions[0].AttemptID != "turn-42:step-0" || streamOptions[1].AttemptID != "turn-42:step-1" {
		t.Fatalf("attempt IDs = %#v", streamOptions)
	}
	if streamOptions[0].TargetRecordID != "message:assistant-1:step-0" || streamOptions[1].TargetRecordID != "message:assistant-1:step-1" {
		t.Fatalf("target record IDs = %#v", streamOptions)
	}
	messageIDs := map[string]bool{}
	for _, event := range records {
		if event.Record.Kind == updates.RecordKindMessage {
			messageIDs[event.Record.RecordID] = true
		}
	}
	if !messageIDs["message:assistant-1:step-0"] || !messageIDs["message:assistant-1:step-1"] {
		t.Fatalf("message records = %#v", records)
	}
}

func TestRecordRetryDoesNotRerunModelOrTool(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var modelCalls, toolCalls, recordCalls int
	env.RegisterActivityWithOptions(func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		modelCalls++
		if modelCalls == 1 {
			return &activities.InvokeModelResult{Content: []activities.Part{{Type: "tool-call", ToolCallID: "call-1", ToolName: "lookup"}}, FinishReason: ai.FinishReason{Unified: ai.FinishToolCalls}}, nil
		}
		return &activities.InvokeModelResult{Content: []activities.Part{{Type: "text", Text: "done"}}, FinishReason: ai.FinishReason{Unified: ai.FinishStop}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})
	env.RegisterActivityWithOptions(func(context.Context, activities.InvokeToolArgs) (*activities.InvokeToolResult, error) {
		toolCalls++
		return &activities.InvokeToolResult{ToolCallID: "call-1", ToolName: "lookup", Output: ai.ToolResultOutput{Type: "text", Value: "ok"}}, nil
	}, activity.RegisterOptions{Name: activities.InvokeToolActivity})
	env.RegisterActivityWithOptions(func(context.Context, activities.WriteRecordArgs) error {
		recordCalls++
		if recordCalls == 1 {
			return temporal.NewApplicationError("transient record failure", "transient")
		}
		return nil
	}, activity.RegisterOptions{Name: activities.WriteRecordActivity})

	env.ExecuteWorkflow(testAgentWorkflowWithRecordRetry, AgentInput{
		ModelID: "model-1", Prompt: "run", Stream: updates.Options{StreamID: "stream-1"}, ToolExecution: ToolExecutionSequential,
		Tools: []activities.ToolDefinition{{Name: "lookup", InputSchema: map[string]any{"type": "object"}}},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if toolCalls != 1 || modelCalls != 2 || recordCalls < 2 {
		t.Fatalf("model=%d tool=%d records=%d", modelCalls, toolCalls, recordCalls)
	}
}

func TestRunAgentDefaultVersionDoesNotScheduleDurableRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.OnGetVersion(durableRecordsChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	var modelCalls int
	env.RegisterActivityWithOptions(func(context.Context, activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
		modelCalls++
		return &activities.InvokeModelResult{
			Content:      []activities.Part{{Type: "text", Text: "done"}},
			FinishReason: ai.FinishReason{Unified: ai.FinishStop},
		}, nil
	}, activity.RegisterOptions{Name: activities.InvokeModelActivity})

	env.ExecuteWorkflow(testAgentWorkflow, AgentInput{
		ModelID: "model-1",
		Prompt:  "run",
		Stream:  updates.Options{StreamID: "stream-1"},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if modelCalls != 1 {
		t.Fatalf("model calls = %d", modelCalls)
	}
}

func TestRequestToolApprovalWritesInteractionRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	inputHash, err := ToolApprovalInputHash(map[string]any{"query": "temporal"})
	if err != nil {
		t.Fatal(err)
	}
	var records []updates.RecordUpsertEvent
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.WriteRecordArgs) error {
		records = append(records, args.Event)
		return nil
	}, activity.RegisterOptions{Name: activities.WriteRecordActivity})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ToolApprovalResponseSignalName("approval-1"), ToolApprovalResponse{ApprovalID: "approval-1", ToolCallID: "wrong-call", InputHash: inputHash, Approved: true})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ToolApprovalResponseSignalName("approval-1"), ToolApprovalResponse{ApprovalID: "approval-1", ToolCallID: "call-1", InputHash: inputHash, Approved: true})
	}, 2*time.Millisecond)
	env.ExecuteWorkflow(testApprovalWorkflow)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Record.Kind != updates.RecordKindInteraction || records[0].Record.Status != "pending" || records[1].Record.Status != "approved" {
		t.Fatalf("records = %#v", records)
	}
	questions, ok := records[0].Record.Data["questions"].([]interface{})
	if !ok || len(questions) != 1 {
		t.Fatalf("questions = %#v", records[0].Record.Data["questions"])
	}
	origin, ok := records[0].Record.Data["origin"].(map[string]any)
	if !ok || origin["toolCallId"] != "call-1" || origin["inputHash"] != inputHash {
		t.Fatalf("approval origin does not bind input: %#v", records[0].Record.Data["origin"])
	}
}

func TestToolApprovalQueryKeepsConcurrentRequests(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(ToolApprovalQueryName)
		if err != nil {
			t.Errorf("query pending approvals: %v", err)
			return
		}
		var snapshot ToolApprovalSnapshot
		if err := value.Get(&snapshot); err != nil {
			t.Errorf("decode pending approvals: %v", err)
			return
		}
		if len(snapshot.Requests) != 2 || !snapshot.Pending {
			t.Errorf("pending snapshot = %#v", snapshot)
			return
		}
		for _, request := range snapshot.Requests {
			env.SignalWorkflow(ToolApprovalResponseSignalName(request.ApprovalID), ToolApprovalResponse{
				ApprovalID: request.ApprovalID, ToolCallID: request.ToolCallID, InputHash: request.InputHash, Approved: true,
			})
		}
	}, time.Millisecond)
	env.ExecuteWorkflow(testConcurrentApprovalsWorkflow)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestRequestToolApprovalDefaultVersionSkipsInteractionRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	inputHash, err := ToolApprovalInputHash(map[string]any{"query": "temporal"})
	if err != nil {
		t.Fatal(err)
	}
	env.OnGetVersion(durableRecordsChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ToolApprovalResponseSignalName("approval-1"), ToolApprovalResponse{ApprovalID: "approval-1", ToolCallID: "call-1", InputHash: inputHash, Approved: true})
	}, time.Millisecond)
	env.ExecuteWorkflow(testApprovalWorkflow)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func testAgentWorkflow(ctx workflow.Context, input AgentInput) (*AgentResult, error) {
	return RunAgent(ctx, input)
}

func testAgentWorkflowWithRecordRetry(ctx workflow.Context, input AgentInput) (*AgentResult, error) {
	return RunAgent(ctx, input, ActivityOptions{Record: workflow.ActivityOptions{
		StartToCloseTimeout: time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Millisecond, MaximumAttempts: 2},
	}})
}

func testApprovalWorkflow(ctx workflow.Context) error {
	response, err := RequestToolApproval(ctx, ToolApprovalRequest{StreamID: "stream-1", ApprovalID: "approval-1", ToolCallID: "call-1", ToolName: "lookup", Input: map[string]any{"query": "temporal"}})
	if err != nil {
		return err
	}
	if !response.Approved {
		return temporal.NewApplicationError("approval was not accepted", "test")
	}
	return nil
}

func testConcurrentApprovalsWorkflow(ctx workflow.Context) error {
	ctx, err := InstallToolApprovalQuery(ctx)
	if err != nil {
		return err
	}
	group := workflow.NewWaitGroup(ctx)
	group.Add(2)
	for _, id := range []string{"approval-a", "approval-b"} {
		id := id
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer group.Done()
			_, _ = RequestToolApproval(ctx, ToolApprovalRequest{
				ApprovalID: id, ToolCallID: "call-" + id, ToolName: "rename", Input: map[string]any{"id": id},
			})
		})
	}
	group.Wait(ctx)
	return nil
}
