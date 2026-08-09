package temporalai

import (
	"encoding/json"
	"fmt"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	defaultAgentMaxSteps = 20
	AgentWorkflowName    = "go-temporal-ai-sdk.AgentWorkflow"

	ToolExecutionParallel   = "parallel"
	ToolExecutionSequential = "sequential"

	LocalToolTimeoutFallbackActivity LocalToolTimeoutFallback = "activity"
	LocalToolTimeoutFallbackNone     LocalToolTimeoutFallback = "none"

	toolArtifactCompactionChange = "go-temporal-ai-sdk.tool-artifact-compaction"
	durableRecordsChange         = "go-temporal-ai-sdk.durable-records-v2"
)

type LocalToolTimeoutFallback string

type AgentInput struct {
	AgentID                  string                              `json:"agentId,omitempty"`
	CompiledRevision         string                              `json:"compiledRevision,omitempty"`
	ModelID                  string                              `json:"modelId"`
	Instructions             string                              `json:"instructions,omitempty"`
	Prompt                   string                              `json:"prompt,omitempty"`
	Messages                 []activities.Message                `json:"messages,omitempty"`
	Tools                    []activities.ToolDefinition         `json:"tools,omitempty"`
	ToolChoice               ai.ToolChoice                       `json:"toolChoice,omitempty"`
	FirstToolChoice          ai.ToolChoice                       `json:"firstToolChoice,omitempty"`
	MaxSteps                 int                                 `json:"maxSteps,omitempty"`
	ModelOptions             activities.LanguageModelCallOptions `json:"modelOptions,omitempty"`
	Stream                   updates.Options                     `json:"stream,omitempty"`
	UseStreamingModel        bool                                `json:"useStreamingModel,omitempty"`
	ToolContext              any                                 `json:"toolContext,omitempty"`
	ToolExecution            string                              `json:"toolExecution,omitempty"`
	ToolApproval             AgentToolApprovalOptions            `json:"toolApproval,omitempty"`
	DefaultModelBoundary     activities.ToolExecutionBoundary    `json:"defaultModelBoundary,omitempty"`
	DefaultToolBoundary      activities.ToolExecutionBoundary    `json:"defaultToolBoundary,omitempty"`
	LocalToolTimeoutFallback LocalToolTimeoutFallback            `json:"localToolTimeoutFallback,omitempty"`
	ToolArtifacts            activities.ToolArtifactPolicy       `json:"toolArtifacts,omitempty"`
	Subagents                []SubagentDefinition                `json:"subagents,omitempty"`
	SubagentExecution        *SubagentExecutionContext           `json:"subagentExecution,omitempty"`
}

type AgentResult struct {
	AgentID          string               `json:"agentId,omitempty"`
	ModelID          string               `json:"modelId"`
	Text             string               `json:"text,omitempty"`
	FinishReason     string               `json:"finishReason,omitempty"`
	RawFinishReason  string               `json:"rawFinishReason,omitempty"`
	Usage            ai.Usage             `json:"usage,omitempty"`
	Warnings         []ai.Warning         `json:"warnings,omitempty"`
	ProviderMetadata ai.ProviderMetadata  `json:"providerMetadata,omitempty"`
	Messages         []activities.Message `json:"messages,omitempty"`
	Steps            []AgentStep          `json:"steps,omitempty"`
}

type AgentStep struct {
	StepID      string                                 `json:"stepId,omitempty"`
	StepNumber  int                                    `json:"stepNumber"`
	StepType    string                                 `json:"stepType,omitempty"`
	ModelResult activities.LanguageModelGenerateResult `json:"modelResult"`
	Text        string                                 `json:"text,omitempty"`
	ToolCalls   []AgentToolCall                        `json:"toolCalls,omitempty"`
	ToolResults []activities.InvokeToolResult          `json:"toolResults,omitempty"`
}

type AgentToolCall struct {
	ToolCallID        string              `json:"toolCallId"`
	ToolName          string              `json:"toolName"`
	StepID            string              `json:"stepId,omitempty"`
	StepNumber        int                 `json:"stepNumber"`
	StepType          string              `json:"stepType,omitempty"`
	Input             any                 `json:"input,omitempty"`
	InputRaw          string              `json:"inputRaw,omitempty"`
	ProviderExecuted  bool                `json:"providerExecuted,omitempty"`
	Dynamic           bool                `json:"dynamic,omitempty"`
	Invalid           bool                `json:"invalid,omitempty"`
	ErrorText         string              `json:"errorText,omitempty"`
	ToolMetadata      ai.ProviderMetadata `json:"toolMetadata,omitempty"`
	ProviderMetadata  ai.ProviderMetadata `json:"providerMetadata,omitempty"`
	AcceptedAttemptID string              `json:"acceptedAttemptId,omitempty"`
}

// AgentWorkflow is the stable root workflow entry point. It owns stream
// termination for root executions only; child subagents report progress to the
// parent but never close the shared root stream.
func AgentWorkflow(ctx workflow.Context, input AgentInput) (*AgentResult, error) {
	result, err := RunAgent(ctx, input)
	if input.SubagentExecution != nil {
		return result, err
	}
	streamID := updateStreamID(ctx, input)
	if streamID == "" {
		return result, err
	}
	outcome := updates.StreamOutcomeCompleted
	errorText := ""
	if err != nil {
		outcome = updates.StreamOutcomeFailed
		errorText = err.Error()
		if temporal.IsCanceledError(err) || temporal.IsCanceledError(ctx.Err()) {
			outcome = updates.StreamOutcomeCanceled
		}
	}
	terminalCtx, _ := workflow.NewDisconnectedContext(ctx)
	if terminalErr := EndStream(terminalCtx, streamID, outcome, errorText); terminalErr != nil && err == nil {
		return result, terminalErr
	}
	return result, err
}

func workflowToolArtifactPolicy(ctx workflow.Context, policy activities.ToolArtifactPolicy) activities.ToolArtifactPolicy {
	if !policy.Enabled {
		return policy
	}
	if workflow.GetVersion(ctx, toolArtifactCompactionChange, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		policy.Enabled = false
		return policy
	}
	info := workflow.GetInfo(ctx)
	if policy.WorkflowID == "" {
		policy.WorkflowID = info.WorkflowExecution.ID
	}
	if policy.RunID == "" {
		policy.RunID = info.WorkflowExecution.RunID
	}
	return policy
}

func toolArtifactPolicyForActivity(policy activities.ToolArtifactPolicy) *activities.ToolArtifactPolicy {
	if !policy.Enabled {
		return nil
	}
	return &policy
}

func durableRecordsEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, durableRecordsChange, workflow.DefaultVersion, 1) != workflow.DefaultVersion
}

func RunAgent(ctx workflow.Context, input AgentInput, activityOptions ...ActivityOptions) (*AgentResult, error) {
	writeRecords := durableRecordsEnabled(ctx)
	if err := publishSubagentProgress(ctx, input, SubagentSnapshot{Status: SubagentStatusRunning, Sequence: 1}, writeRecords, activityOptions...); err != nil {
		return nil, err
	}
	result, err := runAgentLoop(ctx, input, writeRecords, activityOptions...)
	snapshot := SubagentSnapshot{Sequence: 2}
	if result != nil {
		snapshot.Sequence = len(result.Steps) + 2
		snapshot.Text = result.Text
		snapshot.FinishReason = result.FinishReason
		if len(result.Steps) > 0 {
			last := result.Steps[len(result.Steps)-1]
			snapshot.StepNumber = last.StepNumber
			snapshot.StepType = last.StepType
		}
	}
	if err != nil {
		snapshot.Status = SubagentStatusFailed
		snapshot.Error = err.Error()
	} else {
		snapshot.Status = SubagentStatusCompleted
	}
	if progressErr := publishSubagentProgress(ctx, input, snapshot, writeRecords, activityOptions...); progressErr != nil && err == nil {
		return result, progressErr
	}
	return result, err
}

func runAgentLoop(ctx workflow.Context, input AgentInput, writeRecords bool, activityOptions ...ActivityOptions) (*AgentResult, error) {
	if input.ModelID == "" {
		return nil, fmt.Errorf("modelId is required")
	}
	subagents, err := newSubagentManager(ctx, input)
	if err != nil {
		return nil, err
	}
	input.ToolArtifacts = workflowToolArtifactPolicy(ctx, input.ToolArtifacts)
	maxSteps := input.MaxSteps
	if maxSteps <= 0 {
		maxSteps = defaultAgentMaxSteps
	}
	messages := initialAgentMessages(input)
	result := &AgentResult{
		AgentID:  input.AgentID,
		ModelID:  input.ModelID,
		Messages: append([]activities.Message(nil), messages...),
	}
	for stepNumber := 0; stepNumber < maxSteps; stepNumber++ {
		messages = append(messages, drainSubagentMessages(ctx, input)...)
		stepID := agentStepID(stepNumber)
		stepType := agentStepType(stepNumber)
		callOptions := input.ModelOptions
		callOptions.Prompt = append([]activities.Message(nil), messages...)
		toolChoice := input.ToolChoice
		if stepNumber == 0 && input.FirstToolChoice.Type != "" {
			toolChoice = input.FirstToolChoice
		}
		callOptions.Tools = activities.ModelToolsFromDefinitions(agentToolDefinitions(input), toolChoice)
		if toolChoice.Type != "" {
			callOptions.ToolChoice = toolChoice
		} else {
			callOptions.ToolChoice = ai.AutoToolChoice()
		}
		if input.Stream.Visible || input.UseStreamingModel {
			callOptions.ProviderOptions = withAgentStreamOptions(ctx, input, stepID, stepNumber, stepType, callOptions.ProviderOptions)
		}

		modelResult, err := invokeAgentModel(ctx, input, callOptions, activityOptions...)
		if err != nil {
			return nil, err
		}
		step := AgentStep{
			StepID:      stepID,
			StepNumber:  stepNumber,
			StepType:    stepType,
			ModelResult: *modelResult,
			Text:        textFromWireParts(modelResult.Content),
			ToolCalls:   extractToolCalls(modelResult.Content, stepID, stepNumber, stepType),
		}
		attachToolPreviewReceipts(step.ToolCalls, modelResult.PreviewReceipts)
		if writeRecords {
			if err := writeAgentMessageRecords(ctx, input, step, modelResult.PreviewReceipts, activityOptions...); err != nil {
				return nil, err
			}
		}
		result.Text = step.Text
		result.FinishReason = modelResult.FinishReason.Unified
		result.RawFinishReason = modelResult.FinishReason.Raw
		result.Usage = ai.AddUsage(result.Usage, modelResult.Usage)
		result.Warnings = append(result.Warnings, modelResult.Warnings...)
		result.ProviderMetadata = modelResult.ProviderMetadata

		messages = append(messages, activities.Message{Role: ai.RoleAssistant, Content: modelResult.Content})
		result.Messages = append([]activities.Message(nil), messages...)
		if err := publishSubagentStep(ctx, input, step, stepNumber+2, writeRecords, activityOptions...); err != nil {
			return nil, err
		}
		if len(step.ToolCalls) == 0 {
			result.Steps = append(result.Steps, step)
			return result, nil
		}
		toolResults, err := executeAgentTools(ctx, subagents, input, messages, step.ToolCalls, writeRecords, activityOptions...)
		if err != nil {
			return nil, err
		}
		step.ToolResults = toolResults
		result.Steps = append(result.Steps, step)
		if len(toolResults) == 0 {
			return result, nil
		}
		messages = append(messages, activities.Message{Role: ai.RoleTool, Content: toolResultParts(toolResults)})
		result.Messages = append([]activities.Message(nil), messages...)
	}
	return result, nil
}

func publishSubagentStep(ctx workflow.Context, input AgentInput, step AgentStep, sequence int, writeRecords bool, activityOptions ...ActivityOptions) error {
	toolCalls := make([]SubagentToolCallSnapshot, 0, len(step.ToolCalls))
	for _, call := range step.ToolCalls {
		toolCalls = append(toolCalls, SubagentToolCallSnapshot{ToolCallID: call.ToolCallID, ToolName: call.ToolName})
	}
	return publishSubagentProgress(ctx, input, SubagentSnapshot{
		Status:       SubagentStatusRunning,
		Sequence:     sequence,
		StepNumber:   step.StepNumber,
		StepType:     step.StepType,
		Text:         step.Text,
		ToolCalls:    toolCalls,
		FinishReason: step.ModelResult.FinishReason.Unified,
	}, writeRecords, activityOptions...)
}

func ExecuteAgentChildWorkflow(ctx workflow.Context, workflowType any, input AgentInput, options ...workflow.ChildWorkflowOptions) (*AgentResult, error) {
	if len(options) > 0 {
		ctx = workflow.WithChildOptions(ctx, options[0])
	}
	var result AgentResult
	if err := workflow.ExecuteChildWorkflow(ctx, workflowType, input).Get(ctx, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func invokeAgentModel(ctx workflow.Context, input AgentInput, options activities.LanguageModelCallOptions, activityOptions ...ActivityOptions) (*activities.LanguageModelGenerateResult, error) {
	activityOptions = agentActivityOptions(input, activityOptions...)
	if input.UseStreamingModel || input.Stream.Visible {
		streamResult, err := InvokeModelStream(ctx, input.ModelID, options.ToAI(), activityOptions...)
		if err != nil {
			return nil, err
		}
		return generateResultFromStream(streamResult), nil
	}
	result, err := InvokeModel(ctx, input.ModelID, options.ToAI(), activityOptions...)
	if err != nil {
		return nil, err
	}
	wire := activities.GenerateResultFromAI(result)
	return wire, nil
}

func executeAgentTools(ctx workflow.Context, subagents *subagentManager, input AgentInput, messages []activities.Message, calls []AgentToolCall, writeRecords bool, activityOptions ...ActivityOptions) ([]activities.InvokeToolResult, error) {
	if input.ToolExecution == ToolExecutionSequential {
		return executeAgentToolsSequential(ctx, subagents, input, messages, calls, writeRecords, activityOptions...)
	}
	return executeAgentToolsParallel(ctx, subagents, input, messages, calls, writeRecords, activityOptions...)
}

func executeAgentToolsSequential(ctx workflow.Context, subagents *subagentManager, input AgentInput, messages []activities.Message, calls []AgentToolCall, writeRecords bool, activityOptions ...ActivityOptions) ([]activities.InvokeToolResult, error) {
	results := make([]activities.InvokeToolResult, 0, len(calls))
	for _, call := range calls {
		if call.ProviderExecuted {
			continue
		}
		result, err := executeOneAgentTool(ctx, subagents, input, messages, call, writeRecords, activityOptions...)
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	}
	return results, nil
}

func executeAgentToolsParallel(ctx workflow.Context, subagents *subagentManager, input AgentInput, messages []activities.Message, calls []AgentToolCall, writeRecords bool, activityOptions ...ActivityOptions) ([]activities.InvokeToolResult, error) {
	type toolOutcome struct {
		index  int
		result *activities.InvokeToolResult
		err    error
	}
	count := 0
	outcomes := workflow.NewChannel(ctx)
	for _, call := range calls {
		if call.ProviderExecuted {
			continue
		}
		index := count
		toolCall := call
		count++
		workflow.Go(ctx, func(ctx workflow.Context) {
			result, err := executeOneAgentTool(ctx, subagents, input, messages, toolCall, writeRecords, activityOptions...)
			outcomes.Send(ctx, toolOutcome{index: index, result: result, err: err})
		})
	}
	results := make([]activities.InvokeToolResult, count)
	for i := 0; i < count; i++ {
		var outcome toolOutcome
		outcomes.Receive(ctx, &outcome)
		if outcome.err != nil {
			return nil, outcome.err
		}
		if outcome.result != nil {
			results[outcome.index] = *outcome.result
		}
	}
	return results, nil
}

func executeOneAgentTool(ctx workflow.Context, subagents *subagentManager, input AgentInput, messages []activities.Message, call AgentToolCall, writeRecords bool, activityOptions ...ActivityOptions) (*activities.InvokeToolResult, error) {
	if result, handled, err := subagents.execute(ctx, call); handled {
		return result, err
	}
	ao := aoFromActivityOptions(activityOptions...)
	if writeRecords {
		if err := writeAgentToolRecord(ctx, input, call, nil, 1, call.AcceptedAttemptID, ao); err != nil {
			return nil, err
		}
	}
	approval, deniedResult, err := approveAgentToolIfRequired(ctx, input, call, writeRecords, ao)
	if err != nil {
		return nil, err
	}
	if deniedResult != nil {
		if writeRecords {
			if err := writeAgentToolRecord(ctx, input, call, deniedResult, 2, "", ao); err != nil {
				return nil, err
			}
		}
		return deniedResult, nil
	}
	future, boundary := executeAgentToolFuture(ctx, input, messages, call, approval, ao)
	result, err := agentToolResultFromFuture(ctx, input, messages, call, ao, future, boundary, approval)
	if err != nil {
		failed := &activities.InvokeToolResult{ToolCallID: call.ToolCallID, ToolName: call.ToolName, Input: call.Input, IsError: true, Output: ai.ToolResultOutput{Type: "error-text", Value: err.Error()}}
		if writeRecords {
			_ = writeAgentToolRecord(ctx, input, call, failed, 2, "", ao)
		}
		return nil, err
	}
	if writeRecords {
		if err := writeAgentToolRecord(ctx, input, call, result, 2, "", ao); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func executeAgentToolFuture(ctx workflow.Context, input AgentInput, messages []activities.Message, call AgentToolCall, approval *activities.ToolApprovalState, options ActivityOptions) (workflow.Future, activities.ToolExecutionBoundary) {
	args := activities.InvokeToolArgs{
		AgentID:          input.AgentID,
		CompiledRevision: input.CompiledRevision,
		ToolCallID:       call.ToolCallID,
		ToolName:         call.ToolName,
		Input:            call.Input,
		Messages:         messages,
		Context:          input.ToolContext,
		ToolMetadata:     call.ToolMetadata,
		Scope:            agentToolScope(input, call),
		Artifacts:        toolArtifactPolicyForActivity(input.ToolArtifacts),
		Approval:         approval,
	}
	boundary := toolExecutionBoundary(input, call.ToolName)
	switch boundary {
	case activities.ToolExecutionBoundaryLocalActivity:
		toolCtx := workflow.WithLocalActivityOptions(ctx, localToolActivityOptions(options))
		return workflow.ExecuteLocalActivity(toolCtx, activities.InvokeToolActivity, args), boundary
	default:
		return executeAgentToolActivityFuture(ctx, input, args, options), boundary
	}
}

func agentToolResultFromFuture(ctx workflow.Context, input AgentInput, messages []activities.Message, call AgentToolCall, options ActivityOptions, future workflow.Future, boundary activities.ToolExecutionBoundary, approval *activities.ToolApprovalState) (*activities.InvokeToolResult, error) {
	var result activities.InvokeToolResult
	if err := future.Get(ctx, &result); err != nil {
		if shouldFallbackLocalToolTimeout(input, boundary, err) {
			args := activities.InvokeToolArgs{
				AgentID:          input.AgentID,
				CompiledRevision: input.CompiledRevision,
				ToolCallID:       call.ToolCallID,
				ToolName:         call.ToolName,
				Input:            call.Input,
				Messages:         messages,
				Context:          input.ToolContext,
				ToolMetadata:     call.ToolMetadata,
				Scope:            agentToolScope(input, call),
				Artifacts:        toolArtifactPolicyForActivity(input.ToolArtifacts),
				Approval:         approval,
			}
			var fallbackResult activities.InvokeToolResult
			if fallbackErr := executeAgentToolActivityFuture(ctx, input, args, options).Get(ctx, &fallbackResult); fallbackErr != nil {
				return nil, fallbackErr
			}
			return &fallbackResult, nil
		}
		return nil, err
	}
	return &result, nil
}

func executeAgentToolActivityFuture(ctx workflow.Context, input AgentInput, args activities.InvokeToolArgs, options ActivityOptions) workflow.Future {
	if queue := agentToolTaskQueue(input, args.ToolName); queue != "" {
		options.Tool.TaskQueue = queue
	}
	toolCtx := workflow.WithActivityOptions(ctx, toolActivityOptions(options))
	return workflow.ExecuteActivity(toolCtx, activities.InvokeToolActivity, args)
}

func shouldFallbackLocalToolTimeout(input AgentInput, boundary activities.ToolExecutionBoundary, err error) bool {
	return boundary == activities.ToolExecutionBoundaryLocalActivity &&
		localToolTimeoutFallback(input) == LocalToolTimeoutFallbackActivity &&
		temporal.IsTimeoutError(err)
}

func approveAgentToolIfRequired(ctx workflow.Context, input AgentInput, call AgentToolCall, writeRecords bool, options ActivityOptions) (*activities.ToolApprovalState, *activities.InvokeToolResult, error) {
	definition, ok := agentToolDefinition(input, call.ToolName)
	if !ok || !definition.RequiresApproval {
		return nil, nil, nil
	}
	metadata := mergeApprovalMetadata(toolRecordMetadata(input), input.ToolApproval.Metadata)
	approvalID := fmt.Sprintf("%s:approval", call.ToolCallID)
	response, err := requestToolApproval(ctx, ToolApprovalRequest{
		StreamID:     updateStreamID(ctx, input),
		ApprovalID:   approvalID,
		ToolCallID:   call.ToolCallID,
		ToolName:     call.ToolName,
		Input:        call.Input,
		ToolMetadata: call.ToolMetadata,
		Metadata:     metadata,
		Scope:        agentToolScope(input, call),
		Timeout:      input.ToolApproval.Timeout,
		SignalName:   input.ToolApproval.SignalName,
	}, writeRecords, options)
	if err != nil {
		return nil, nil, err
	}
	approval := toolApprovalState(response)
	if response.Approved {
		return approval, nil, nil
	}
	return approval, deniedAgentToolResult(call, response.Reason), nil
}

func deniedAgentToolResult(call AgentToolCall, reason string) *activities.InvokeToolResult {
	return &activities.InvokeToolResult{
		ToolCallID:       call.ToolCallID,
		ToolName:         call.ToolName,
		Input:            call.Input,
		Output:           ai.ToolResultOutput{Type: "execution-denied", Reason: reason},
		Dynamic:          call.Dynamic,
		ToolMetadata:     call.ToolMetadata,
		ProviderMetadata: call.ProviderMetadata,
	}
}

func agentToolDefinition(input AgentInput, toolName string) (activities.ToolDefinition, bool) {
	for _, tool := range input.Tools {
		if tool.Name == toolName {
			return tool, true
		}
	}
	return activities.ToolDefinition{}, false
}

func mergeApprovalMetadata(base map[string]any, extra map[string]any) map[string]any {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range base {
		out[key] = value
	}
	for key, value := range extra {
		out[key] = value
	}
	return out
}

func localToolTimeoutFallback(input AgentInput) LocalToolTimeoutFallback {
	if input.LocalToolTimeoutFallback == "" {
		return LocalToolTimeoutFallbackActivity
	}
	return input.LocalToolTimeoutFallback
}

func aoFromActivityOptions(activityOptions ...ActivityOptions) ActivityOptions {
	if len(activityOptions) > 0 {
		return activityOptions[0]
	}
	return ActivityOptions{}
}

func agentActivityOptions(input AgentInput, activityOptions ...ActivityOptions) []ActivityOptions {
	options := aoFromActivityOptions(activityOptions...)
	options.AgentID = input.AgentID
	options.CompiledRevision = input.CompiledRevision
	if input.DefaultModelBoundary != "" && input.DefaultModelBoundary != activities.ToolExecutionBoundaryAuto {
		options.LanguageModelBoundary = input.DefaultModelBoundary
	}
	return []ActivityOptions{options}
}

func agentToolTaskQueue(input AgentInput, toolName string) string {
	for _, tool := range input.Tools {
		if tool.Name == toolName {
			return tool.TaskQueue
		}
	}
	return ""
}

func toolExecutionBoundary(input AgentInput, toolName string) activities.ToolExecutionBoundary {
	for _, tool := range input.Tools {
		if tool.Name != toolName {
			continue
		}
		if tool.ExecutionBoundary != "" && tool.ExecutionBoundary != activities.ToolExecutionBoundaryAuto {
			return tool.ExecutionBoundary
		}
		break
	}
	if input.DefaultToolBoundary != "" && input.DefaultToolBoundary != activities.ToolExecutionBoundaryAuto {
		return input.DefaultToolBoundary
	}
	return activities.ToolExecutionBoundaryActivity
}

func initialAgentMessages(input AgentInput) []activities.Message {
	messages := make([]activities.Message, 0, len(input.Messages)+2)
	if input.Instructions != "" {
		messages = append(messages, activities.Message{Role: ai.RoleSystem, Text: input.Instructions})
	}
	messages = append(messages, input.Messages...)
	if input.Prompt != "" {
		messages = append(messages, activities.Message{
			Role:    ai.RoleUser,
			Content: []activities.Part{{Type: "text", Text: input.Prompt}},
		})
	}
	return messages
}

func withAgentStreamOptions(ctx workflow.Context, input AgentInput, stepID string, stepNumber int, stepType string, providerOptions ai.ProviderOptions) ai.ProviderOptions {
	out := ai.ProviderOptions{}
	for key, value := range providerOptions {
		out[key] = value
	}
	options := input.Stream
	options.Scope = agentStepScope(input, stepID, stepNumber, stepType)
	if options.StreamID == "" {
		options.StreamID = streamID(ctx, "")
	}
	targetRecordBase := options.TargetRecordID
	if targetRecordBase == "" {
		targetRecordBase = fmt.Sprintf("message:%s", options.StreamID)
	}
	options.TargetRecordID = fmt.Sprintf("%s:%s", targetRecordBase, stepID)
	attemptBase := options.AttemptID
	if attemptBase == "" {
		agentID := input.AgentID
		if agentID == "" {
			agentID = "agent"
		}
		attemptBase = agentID
	}
	options.AttemptID = fmt.Sprintf("%s:%s", attemptBase, stepID)
	if !options.Visible && input.UseStreamingModel {
		options.Visible = true
	}
	out[activities.ProviderOptionsKey] = options
	return out
}

func agentStepID(stepNumber int) string {
	return fmt.Sprintf("step-%d", stepNumber)
}

func agentStepType(stepNumber int) string {
	if stepNumber == 0 {
		return "initial"
	}
	return "tool-result"
}

func agentStepScope(input AgentInput, stepID string, stepNumber int, stepType string) updates.Scope {
	scope := input.Stream.Scope
	toolContextScope := streamScopeFromContext(input.ToolContext)
	if scope.AgentID == "" {
		scope.AgentID = input.AgentID
	}
	if scope.TaskID == "" {
		scope.TaskID = toolContextScope.TaskID
	}
	if scope.TaskTitle == "" {
		scope.TaskTitle = toolContextScope.TaskTitle
	}
	if scope.SkillName == "" {
		scope.SkillName = toolContextScope.SkillName
	}
	scope.StepID = stepID
	scope.StepNumber = intPtr(stepNumber)
	scope.StepType = stepType
	return scope
}

func agentToolScope(input AgentInput, call AgentToolCall) updates.Scope {
	stepNumber := call.StepNumber
	return agentStepScope(input, call.StepID, stepNumber, call.StepType)
}

func streamScopeFromContext(context any) updates.Scope {
	metadata := toolLifecycleMetadataFromContext(context)
	return updates.Scope{
		TaskID:    stringFromMetadata(metadata, "taskId"),
		TaskTitle: stringFromMetadata(metadata, "taskTitle"),
		SkillName: stringFromMetadata(metadata, "skillName"),
	}
}

func stringFromMetadata(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

func intPtr(value int) *int {
	return &value
}

func streamID(ctx workflow.Context, configured string) string {
	if configured != "" {
		return configured
	}
	return workflow.GetInfo(ctx).WorkflowExecution.ID
}

func updateStreamID(ctx workflow.Context, input AgentInput) string {
	if !input.Stream.Visible && input.Stream.StreamID == "" && !input.UseStreamingModel {
		return ""
	}
	return streamID(ctx, input.Stream.StreamID)
}

func toolRecordMetadata(input AgentInput) map[string]any {
	metadata := map[string]any{"agentId": input.AgentID}
	for key, value := range toolLifecycleMetadataFromContext(input.ToolContext) {
		metadata[key] = value
	}
	return metadata
}

func attachToolPreviewReceipts(calls []AgentToolCall, receipts []updates.PreviewReceipt) {
	for i := range calls {
		target := "tool:" + calls[i].ToolCallID
		for _, receipt := range receipts {
			if receipt.Outcome == updates.PreviewOutcomeSucceeded && receipt.TargetRecordID == target && receipt.Lane == updates.LaneToolInput {
				calls[i].AcceptedAttemptID = receipt.AttemptID
				break
			}
		}
	}
}

func writeAgentMessageRecords(ctx workflow.Context, input AgentInput, step AgentStep, receipts []updates.PreviewReceipt, activityOptions ...ActivityOptions) error {
	streamID := updateStreamID(ctx, input)
	if streamID == "" {
		return nil
	}
	wrote := false
	for _, receipt := range receipts {
		if receipt.Outcome != updates.PreviewOutcomeSucceeded || (receipt.Lane != updates.LaneText && receipt.Lane != updates.LaneReasoning && receipt.Lane != updates.LaneObject) {
			continue
		}
		data := map[string]any{
			"messageId": receipt.TargetRecordID,
			"role":      "assistant",
		}
		switch receipt.Lane {
		case updates.LaneReasoning:
			data["reasoning"] = receipt.Snapshot.Text
		case updates.LaneObject:
			data["object"] = receipt.Snapshot.Object
			if len(receipt.Snapshot.Elements) > 0 {
				data["elements"] = receipt.Snapshot.Elements
			}
		default:
			data["text"] = receipt.Snapshot.Text
		}
		record := updates.WorkflowRecord{
			RecordID:      receipt.TargetRecordID,
			RecordVersion: 1,
			Kind:          updates.RecordKindMessage,
			Status:        "completed",
			Data:          data,
			Scope:         receipt.Scope,
		}
		if err := WriteRecord(ctx, streamID, record, receipt.AttemptID, activityOptions...); err != nil {
			return err
		}
		wrote = true
	}
	if wrote {
		return nil
	}
	recordID := input.Stream.TargetRecordID
	if recordID == "" {
		recordID = fmt.Sprintf("message:%s", streamID)
	}
	recordID = fmt.Sprintf("%s:%s", recordID, step.StepID)
	return WriteRecord(ctx, streamID, updates.WorkflowRecord{
		RecordID:      recordID,
		RecordVersion: 1,
		Kind:          updates.RecordKindMessage,
		Status:        "completed",
		Data: map[string]any{
			"messageId": recordID,
			"role":      "assistant",
			"text":      step.Text,
		},
		Scope: agentStepScope(input, step.StepID, step.StepNumber, step.StepType),
	}, "", activityOptions...)
}

func writeAgentToolRecord(ctx workflow.Context, input AgentInput, call AgentToolCall, result *activities.InvokeToolResult, version int, acceptedAttemptID string, activityOptions ...ActivityOptions) error {
	streamID := updateStreamID(ctx, input)
	if streamID == "" {
		return nil
	}
	status := "running"
	data := map[string]any{
		"toolCallId":       call.ToolCallID,
		"toolName":         call.ToolName,
		"input":            call.Input,
		"dynamic":          call.Dynamic,
		"providerExecuted": call.ProviderExecuted,
	}
	if len(call.ToolMetadata) > 0 {
		data["toolMetadata"] = call.ToolMetadata
	}
	if len(call.ProviderMetadata) > 0 {
		data["providerMetadata"] = call.ProviderMetadata
	}
	if result != nil {
		status = "succeeded"
		data["output"] = result.Output
		data["result"] = result.Result
		data["preliminary"] = result.Preliminary
		if result.Output.Type == "execution-denied" {
			status = "denied"
		} else if result.IsError {
			status = "failed"
		}
	}
	return WriteRecord(ctx, streamID, updates.WorkflowRecord{
		RecordID:      "tool:" + call.ToolCallID,
		RecordVersion: version,
		Kind:          updates.RecordKindTool,
		Status:        status,
		Data:          data,
		Scope:         agentToolScope(input, call),
	}, acceptedAttemptID, activityOptions...)
}

func toolLifecycleMetadataFromContext(context any) map[string]any {
	if context == nil {
		return nil
	}
	var raw map[string]any
	switch value := context.(type) {
	case map[string]any:
		raw = value
	default:
		payload, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		if err := json.Unmarshal(payload, &raw); err != nil {
			return nil
		}
	}
	metadata := map[string]any{}
	copyStringMetadata(metadata, raw, "taskId")
	copyStringMetadata(metadata, raw, "taskTitle")
	copyStringMetadata(metadata, raw, "skillName")
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func copyStringMetadata(out map[string]any, raw map[string]any, key string) {
	value, ok := raw[key].(string)
	if !ok || value == "" {
		return
	}
	out[key] = value
}

func extractToolCalls(parts []activities.Part, stepID string, stepNumber int, stepType string) []AgentToolCall {
	calls := []AgentToolCall{}
	for _, part := range parts {
		if part.Type != "tool-call" {
			continue
		}
		input := part.Input
		errorText := part.ErrorText
		invalid := part.Invalid
		if input == nil && part.InputRaw != "" {
			var parsed any
			if err := json.Unmarshal([]byte(part.InputRaw), &parsed); err != nil {
				input = part.InputRaw
				errorText = err.Error()
				invalid = true
			} else {
				input = parsed
			}
		}
		calls = append(calls, AgentToolCall{
			ToolCallID:       part.ToolCallID,
			ToolName:         part.ToolName,
			StepID:           stepID,
			StepNumber:       stepNumber,
			StepType:         stepType,
			Input:            input,
			InputRaw:         part.InputRaw,
			ProviderExecuted: part.ProviderExecuted,
			Dynamic:          part.Dynamic,
			Invalid:          invalid,
			ErrorText:        errorText,
			ToolMetadata:     part.ToolMetadata,
			ProviderMetadata: part.ProviderMetadata,
		})
	}
	return calls
}

func toolResultParts(results []activities.InvokeToolResult) []activities.Part {
	parts := make([]activities.Part, 0, len(results))
	for _, result := range results {
		parts = append(parts, activities.Part{
			Type:             "tool-result",
			ToolCallID:       result.ToolCallID,
			ToolName:         result.ToolName,
			Input:            result.Input,
			Output:           result.Output,
			Result:           result.Result,
			IsError:          result.IsError,
			Dynamic:          result.Dynamic,
			ProviderExecuted: result.ProviderExecuted,
			Preliminary:      result.Preliminary,
			ToolMetadata:     result.ToolMetadata,
			ProviderMetadata: result.ProviderMetadata,
		})
	}
	return parts
}

func textFromWireParts(parts []activities.Part) string {
	var out string
	for _, part := range parts {
		if part.Type == "text" {
			out += part.Text
		}
	}
	return out
}

func generateResultFromStream(result *activities.InvokeModelStreamAIResult) *activities.LanguageModelGenerateResult {
	if result.Result != nil {
		wire := activities.GenerateResultFromAI(result.Result)
		wire.PreviewReceipts = append([]updates.PreviewReceipt(nil), result.PreviewReceipts...)
		return wire
	}
	out := &activities.LanguageModelGenerateResult{
		Request:         result.Request,
		Response:        activities.ResponseMetadataFromAI(result.Response),
		PreviewReceipts: append([]updates.PreviewReceipt(nil), result.PreviewReceipts...),
	}
	var text string
	var reasoning string
	toolInputs := map[string]string{}
	for _, part := range result.StreamParts {
		switch part.Type {
		case "text-delta":
			text += part.TextDelta
		case "reasoning-delta":
			reasoning += part.ReasoningDelta
		case "tool-input-delta":
			toolInputs[part.ToolCallID] += part.ToolInputDelta
		case "tool-input-end":
			if part.ToolInput != "" {
				toolInputs[part.ToolCallID] = part.ToolInput
			}
		case "tool-call":
			input := part.ToolInput
			if input == "" {
				input = toolInputs[part.ToolCallID]
			}
			out.Content = append(out.Content, activities.Part{
				Type:             "tool-call",
				ToolCallID:       part.ToolCallID,
				ToolName:         part.ToolName,
				Input:            toolCallInputFromRaw(input),
				InputRaw:         input,
				ToolMetadata:     part.ToolMetadata,
				ProviderMetadata: part.ProviderMetadata,
			})
		case "reasoning-file", "file", "source":
			if part.Content != nil {
				out.Content = append(out.Content, activities.PartFromAI(part.Content))
			}
		case "finish":
			out.FinishReason = part.FinishReason
			out.Usage = part.Usage
			out.Warnings = append(out.Warnings, part.Warnings...)
			out.ProviderMetadata = part.ProviderMetadata
		}
	}
	if text != "" {
		out.Content = append([]activities.Part{{Type: "text", Text: text}}, out.Content...)
	}
	if reasoning != "" {
		out.Content = append([]activities.Part{{Type: "reasoning", Text: reasoning}}, out.Content...)
	}
	return out
}

func toolCallInputFromRaw(inputRaw string) any {
	if inputRaw == "" {
		return nil
	}
	var parsed any
	if err := json.Unmarshal([]byte(inputRaw), &parsed); err != nil {
		return nil
	}
	return parsed
}
