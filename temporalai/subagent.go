package temporalai

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

const (
	SubagentProgressSignalName = "go-temporal-ai-sdk.subagent-progress"
	SubagentMessageSignalName  = "go-temporal-ai-sdk.subagent-message"
	SubagentsQueryName         = "go-temporal-ai-sdk.subagents"

	ListSubagentsToolName    = "list_subagents"
	InspectSubagentToolName  = "inspect_subagent"
	WaitSubagentToolName     = "wait_subagent"
	MessageSubagentToolName  = "message_subagent"
	CancelSubagentToolName   = "cancel_subagent"
	defaultSubagentWait      = 30 * time.Second
	defaultSubagentTaskField = "task"
)

type SubagentStatus string

const (
	SubagentStatusStarting   SubagentStatus = "starting"
	SubagentStatusRunning    SubagentStatus = "running"
	SubagentStatusCancelling SubagentStatus = "cancelling"
	SubagentStatusCompleted  SubagentStatus = "completed"
	SubagentStatusFailed     SubagentStatus = "failed"
	SubagentStatusCanceled   SubagentStatus = "canceled"
)

// SubagentDefinition exposes a child AgentWorkflow to a parent model as a tool.
// Agent is copied for each invocation and receives the tool's task as its prompt.
type SubagentDefinition struct {
	Tool              activities.ToolDefinition `json:"tool"`
	Agent             *AgentInput               `json:"agent,omitempty"`
	WorkflowType      string                    `json:"workflowType,omitempty"`
	TaskQueue         string                    `json:"taskQueue,omitempty"`
	ParentClosePolicy enumspb.ParentClosePolicy `json:"parentClosePolicy,omitempty"`
}

type SubagentExecutionContext struct {
	SubagentID       string `json:"subagentId"`
	ToolCallID       string `json:"toolCallId"`
	ToolName         string `json:"toolName"`
	ParentWorkflowID string `json:"parentWorkflowId"`
	ParentRunID      string `json:"parentRunId,omitempty"`
}

type SubagentSnapshot struct {
	SubagentID   string                     `json:"subagentId"`
	ToolCallID   string                     `json:"toolCallId"`
	ToolName     string                     `json:"toolName"`
	WorkflowID   string                     `json:"workflowId,omitempty"`
	RunID        string                     `json:"runId,omitempty"`
	Status       SubagentStatus             `json:"status"`
	Sequence     int                        `json:"sequence"`
	StepNumber   int                        `json:"stepNumber,omitempty"`
	StepType     string                     `json:"stepType,omitempty"`
	Text         string                     `json:"text,omitempty"`
	ToolCalls    []SubagentToolCallSnapshot `json:"toolCalls,omitempty"`
	FinishReason string                     `json:"finishReason,omitempty"`
	Error        string                     `json:"error,omitempty"`
	UpdatedAt    time.Time                  `json:"updatedAt"`
}

type SubagentToolCallSnapshot struct {
	ToolCallID string `json:"toolCallId"`
	ToolName   string `json:"toolName"`
}

type SubagentMessage struct {
	Content string `json:"content"`
}

type SubagentWaitResult struct {
	Snapshot SubagentSnapshot `json:"snapshot"`
	TimedOut bool             `json:"timedOut,omitempty"`
}

type subagentToolInput struct {
	SubagentID    string `json:"subagentId,omitempty"`
	Task          string `json:"task,omitempty"`
	Message       string `json:"message,omitempty"`
	Reason        string `json:"reason,omitempty"`
	SinceSequence int    `json:"sinceSequence,omitempty"`
	TimeoutSecond int    `json:"timeoutSeconds,omitempty"`
}

type subagentRuntime struct {
	definition SubagentDefinition
	future     workflow.ChildWorkflowFuture
	cancel     workflow.CancelFunc
	snapshot   SubagentSnapshot
	result     *AgentResult
}

type subagentManager struct {
	ctx         workflow.Context
	parentInput AgentInput
	definitions map[string]SubagentDefinition
	runtimes    map[string]*subagentRuntime
}

func newSubagentManager(ctx workflow.Context, input AgentInput) (*subagentManager, error) {
	if len(input.Subagents) == 0 {
		return nil, nil
	}
	manager := &subagentManager{
		ctx:         ctx,
		parentInput: input,
		definitions: make(map[string]SubagentDefinition, len(input.Subagents)),
		runtimes:    map[string]*subagentRuntime{},
	}
	reserved := subagentReservedToolNames()
	for _, definition := range input.Subagents {
		name := definition.Tool.Name
		if name == "" {
			return nil, fmt.Errorf("subagent tool name is required")
		}
		if definition.Tool.RequiresApproval {
			return nil, fmt.Errorf("subagent tool %q cannot require approval", name)
		}
		if _, ok := reserved[name]; ok {
			return nil, fmt.Errorf("subagent tool name %q is reserved", name)
		}
		if _, ok := manager.definitions[name]; ok {
			return nil, fmt.Errorf("duplicate subagent tool name %q", name)
		}
		manager.definitions[name] = definition
	}
	for _, tool := range input.Tools {
		if _, ok := manager.definitions[tool.Name]; ok {
			return nil, fmt.Errorf("tool %q is defined as both a tool and subagent", tool.Name)
		}
		if _, ok := reserved[tool.Name]; ok {
			return nil, fmt.Errorf("tool name %q is reserved for subagent orchestration", tool.Name)
		}
	}
	manager.listenForProgress()
	if err := workflow.SetQueryHandler(ctx, SubagentsQueryName, func() ([]SubagentSnapshot, error) {
		return manager.list(), nil
	}); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *subagentManager) listenForProgress() {
	progress := workflow.GetSignalChannel(m.ctx, SubagentProgressSignalName)
	workflow.Go(m.ctx, func(ctx workflow.Context) {
		for {
			var snapshot SubagentSnapshot
			progress.Receive(ctx, &snapshot)
			m.applySnapshot(snapshot)
		}
	})
}

func (m *subagentManager) applySnapshot(snapshot SubagentSnapshot) {
	runtime := m.runtimes[snapshot.SubagentID]
	if runtime == nil {
		return
	}
	current := runtime.snapshot
	if snapshot.Sequence <= current.Sequence {
		snapshot.Sequence = current.Sequence + 1
	}
	if snapshot.WorkflowID == "" {
		snapshot.WorkflowID = current.WorkflowID
	}
	if snapshot.RunID == "" {
		snapshot.RunID = current.RunID
	}
	if snapshot.ToolCallID == "" {
		snapshot.ToolCallID = current.ToolCallID
	}
	if snapshot.ToolName == "" {
		snapshot.ToolName = current.ToolName
	}
	if snapshot.UpdatedAt.IsZero() {
		snapshot.UpdatedAt = workflow.Now(m.ctx)
	}
	runtime.snapshot = snapshot
}

func (m *subagentManager) execute(ctx workflow.Context, call AgentToolCall) (*activities.InvokeToolResult, bool, error) {
	if m == nil {
		return nil, false, nil
	}
	if definition, ok := m.definitions[call.ToolName]; ok {
		result, err := m.spawn(ctx, call, definition)
		return result, true, err
	}
	input, err := decodeSubagentToolInput(call.Input)
	if err != nil {
		return nil, true, subagentToolError(call, err)
	}
	switch call.ToolName {
	case ListSubagentsToolName:
		return subagentToolResult(call, m.list()), true, nil
	case InspectSubagentToolName:
		snapshot, err := m.inspect(input.SubagentID)
		if err != nil {
			return nil, true, subagentToolError(call, err)
		}
		return subagentToolResult(call, snapshot), true, nil
	case WaitSubagentToolName:
		result, err := m.wait(ctx, input)
		if err != nil {
			return nil, true, subagentToolError(call, err)
		}
		return subagentToolResult(call, result), true, nil
	case MessageSubagentToolName:
		snapshot, err := m.message(ctx, input.SubagentID, input.Message)
		if err != nil {
			return nil, true, subagentToolError(call, err)
		}
		return subagentToolResult(call, snapshot), true, nil
	case CancelSubagentToolName:
		snapshot, err := m.cancel(input.SubagentID, input.Reason)
		if err != nil {
			return nil, true, subagentToolError(call, err)
		}
		return subagentToolResult(call, snapshot), true, nil
	default:
		return nil, false, nil
	}
}

func (m *subagentManager) spawn(ctx workflow.Context, call AgentToolCall, definition SubagentDefinition) (*activities.InvokeToolResult, error) {
	subagentID := subagentIDForCall(call)
	if runtime := m.runtimes[subagentID]; runtime != nil {
		return subagentToolResult(call, runtime.snapshot), nil
	}
	childInput := subagentInputForCall(m.parentInput, definition, call, subagentID)
	info := workflow.GetInfo(ctx)
	childInput.SubagentExecution = &SubagentExecutionContext{
		SubagentID:       subagentID,
		ToolCallID:       call.ToolCallID,
		ToolName:         call.ToolName,
		ParentWorkflowID: info.WorkflowExecution.ID,
		ParentRunID:      info.WorkflowExecution.RunID,
	}
	childCtx, cancel := workflow.WithCancel(ctx)
	options := workflow.ChildWorkflowOptions{
		WorkflowID:        subagentWorkflowID(info.WorkflowExecution.ID, subagentID),
		TaskQueue:         definition.TaskQueue,
		ParentClosePolicy: definition.ParentClosePolicy,
	}
	childCtx = workflow.WithChildOptions(childCtx, options)
	workflowType := any(AgentWorkflowName)
	if definition.WorkflowType != "" {
		workflowType = definition.WorkflowType
	}
	future := workflow.ExecuteChildWorkflow(childCtx, workflowType, childInput)
	runtime := &subagentRuntime{
		definition: definition,
		future:     future,
		cancel:     cancel,
		snapshot: SubagentSnapshot{
			SubagentID: subagentID,
			ToolCallID: call.ToolCallID,
			ToolName:   call.ToolName,
			WorkflowID: options.WorkflowID,
			Status:     SubagentStatusStarting,
			Sequence:   1,
			UpdatedAt:  workflow.Now(ctx),
		},
	}
	m.runtimes[subagentID] = runtime
	var execution workflow.Execution
	if err := future.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
		delete(m.runtimes, subagentID)
		cancel()
		return nil, err
	}
	runtime.snapshot.WorkflowID = execution.ID
	runtime.snapshot.RunID = execution.RunID
	runtime.snapshot.Status = SubagentStatusRunning
	runtime.snapshot.Sequence++
	runtime.snapshot.UpdatedAt = workflow.Now(ctx)
	m.monitor(ctx, runtime)
	return subagentToolResult(call, runtime.snapshot), nil
}

func (m *subagentManager) monitor(ctx workflow.Context, runtime *subagentRuntime) {
	workflow.Go(ctx, func(ctx workflow.Context) {
		var result AgentResult
		err := runtime.future.Get(ctx, &result)
		snapshot := runtime.snapshot
		snapshot.Sequence++
		snapshot.UpdatedAt = workflow.Now(ctx)
		if err != nil {
			if snapshot.Status == SubagentStatusCancelling {
				snapshot.Status = SubagentStatusCanceled
			} else {
				snapshot.Status = SubagentStatusFailed
			}
			snapshot.Error = err.Error()
		} else {
			runtime.result = &result
			snapshot.Status = SubagentStatusCompleted
			snapshot.Text = result.Text
			snapshot.FinishReason = result.FinishReason
			if len(result.Steps) > 0 {
				last := result.Steps[len(result.Steps)-1]
				snapshot.StepNumber = last.StepNumber
				snapshot.StepType = last.StepType
			}
		}
		runtime.snapshot = snapshot
	})
}

func (m *subagentManager) list() []SubagentSnapshot {
	ids := make([]string, 0, len(m.runtimes))
	for id := range m.runtimes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]SubagentSnapshot, 0, len(ids))
	for _, id := range ids {
		out = append(out, m.runtimes[id].snapshot)
	}
	return out
}

func (m *subagentManager) inspect(id string) (SubagentSnapshot, error) {
	if id == "" {
		return SubagentSnapshot{}, fmt.Errorf("subagentId is required")
	}
	runtime := m.runtimes[id]
	if runtime == nil {
		return SubagentSnapshot{}, fmt.Errorf("subagent %q not found", id)
	}
	return runtime.snapshot, nil
}

func (m *subagentManager) wait(ctx workflow.Context, input subagentToolInput) (SubagentWaitResult, error) {
	runtime := m.runtimes[input.SubagentID]
	if runtime == nil {
		return SubagentWaitResult{}, fmt.Errorf("subagent %q not found", input.SubagentID)
	}
	if subagentTerminal(runtime.snapshot.Status) {
		return SubagentWaitResult{Snapshot: runtime.snapshot}, nil
	}
	since := input.SinceSequence
	if since <= 0 {
		since = runtime.snapshot.Sequence
	}
	timeout := defaultSubagentWait
	if input.TimeoutSecond > 0 {
		timeout = time.Duration(input.TimeoutSecond) * time.Second
	}
	ok, err := workflow.AwaitWithTimeout(ctx, timeout, func() bool {
		return runtime.snapshot.Sequence > since || subagentTerminal(runtime.snapshot.Status)
	})
	if err != nil {
		return SubagentWaitResult{}, err
	}
	return SubagentWaitResult{Snapshot: runtime.snapshot, TimedOut: !ok}, nil
}

func (m *subagentManager) message(ctx workflow.Context, id string, message string) (SubagentSnapshot, error) {
	runtime := m.runtimes[id]
	if runtime == nil {
		return SubagentSnapshot{}, fmt.Errorf("subagent %q not found", id)
	}
	if strings.TrimSpace(message) == "" {
		return SubagentSnapshot{}, fmt.Errorf("message is required")
	}
	if subagentTerminal(runtime.snapshot.Status) {
		return SubagentSnapshot{}, fmt.Errorf("subagent %q is %s", id, runtime.snapshot.Status)
	}
	if err := runtime.future.SignalChildWorkflow(ctx, SubagentMessageSignalName, SubagentMessage{Content: message}).Get(ctx, nil); err != nil {
		return SubagentSnapshot{}, err
	}
	return runtime.snapshot, nil
}

func (m *subagentManager) cancel(id string, reason string) (SubagentSnapshot, error) {
	runtime := m.runtimes[id]
	if runtime == nil {
		return SubagentSnapshot{}, fmt.Errorf("subagent %q not found", id)
	}
	if subagentTerminal(runtime.snapshot.Status) {
		return runtime.snapshot, nil
	}
	runtime.snapshot.Status = SubagentStatusCancelling
	runtime.snapshot.Sequence++
	runtime.snapshot.UpdatedAt = workflow.Now(m.ctx)
	if reason != "" {
		runtime.snapshot.Error = reason
	}
	runtime.cancel()
	return runtime.snapshot, nil
}

func subagentInputForCall(parent AgentInput, definition SubagentDefinition, call AgentToolCall, subagentID string) AgentInput {
	input := AgentInput{}
	if definition.Agent != nil {
		input = *definition.Agent
	}
	if input.AgentID == "" {
		input.AgentID = subagentID
	}
	input.Prompt = subagentTask(call.Input)
	if input.Stream.StreamID == "" {
		input.Stream.StreamID = parent.Stream.StreamID
	}
	if !input.Stream.Visible && parent.Stream.Visible {
		input.Stream.Visible = true
	}
	if input.Stream.DisplayMode == "" {
		input.Stream.DisplayMode = parent.Stream.DisplayMode
	}
	input.Stream.AgentID = input.AgentID
	if input.Stream.TaskID == "" {
		input.Stream.TaskID = call.ToolCallID
	}
	if input.Stream.TaskTitle == "" {
		input.Stream.TaskTitle = call.ToolName
	}
	return input
}

func agentToolDefinitions(input AgentInput) []activities.ToolDefinition {
	definitions := append([]activities.ToolDefinition(nil), input.Tools...)
	if len(input.Subagents) == 0 {
		return definitions
	}
	for _, subagent := range input.Subagents {
		tool := subagent.Tool
		if tool.Description == "" {
			tool.Description = "Start the " + tool.Name + " subagent in the background and return its handle."
		}
		if tool.InputSchema == nil {
			tool.InputSchema = objectSchema(map[string]any{
				defaultSubagentTaskField: map[string]any{"type": "string", "description": "Task for the subagent."},
			}, []any{defaultSubagentTaskField})
		}
		definitions = append(definitions, tool)
	}
	definitions = append(definitions, subagentControlToolDefinitions()...)
	return definitions
}

func subagentControlToolDefinitions() []activities.ToolDefinition {
	idSchema := map[string]any{"type": "string", "description": "Subagent handle returned by a subagent tool."}
	return []activities.ToolDefinition{
		{Name: ListSubagentsToolName, Description: "List background subagents and their latest durable status.", InputSchema: objectSchema(nil, nil)},
		{Name: InspectSubagentToolName, Description: "Inspect the latest durable snapshot of a background subagent.", InputSchema: objectSchema(map[string]any{"subagentId": idSchema}, []any{"subagentId"})},
		{Name: WaitSubagentToolName, Description: "Wait for a background subagent to make progress or finish.", InputSchema: objectSchema(map[string]any{"subagentId": idSchema, "sinceSequence": map[string]any{"type": "integer"}, "timeoutSeconds": map[string]any{"type": "integer"}}, []any{"subagentId"})},
		{Name: MessageSubagentToolName, Description: "Send additional instructions to a running subagent.", InputSchema: objectSchema(map[string]any{"subagentId": idSchema, "message": map[string]any{"type": "string"}}, []any{"subagentId", "message"})},
		{Name: CancelSubagentToolName, Description: "Cancel a running subagent.", InputSchema: objectSchema(map[string]any{"subagentId": idSchema, "reason": map[string]any{"type": "string"}}, []any{"subagentId"})},
	}
}

func objectSchema(properties map[string]any, required []any) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func subagentReservedToolNames() map[string]struct{} {
	return map[string]struct{}{
		ListSubagentsToolName: {}, InspectSubagentToolName: {}, WaitSubagentToolName: {}, MessageSubagentToolName: {}, CancelSubagentToolName: {},
	}
}

func decodeSubagentToolInput(value any) (subagentToolInput, error) {
	if value == nil {
		return subagentToolInput{}, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return subagentToolInput{}, err
	}
	var input subagentToolInput
	if err := json.Unmarshal(data, &input); err != nil {
		return subagentToolInput{}, err
	}
	return input, nil
}

func subagentTask(value any) string {
	if input, err := decodeSubagentToolInput(value); err == nil && input.Task != "" {
		return input.Task
	}
	if text, ok := value.(string); ok {
		return text
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

func subagentIDForCall(call AgentToolCall) string {
	return sanitizeSubagentID(call.ToolName + "-" + call.ToolCallID)
}

func subagentWorkflowID(parentWorkflowID string, subagentID string) string {
	return parentWorkflowID + ":subagent:" + subagentID
}

func sanitizeSubagentID(value string) string {
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteRune('-')
		}
	}
	result := strings.Trim(builder.String(), "-")
	if result == "" {
		return "subagent"
	}
	return result
}

func subagentTerminal(status SubagentStatus) bool {
	switch status {
	case SubagentStatusCompleted, SubagentStatusFailed, SubagentStatusCanceled:
		return true
	default:
		return false
	}
}

func subagentToolResult(call AgentToolCall, value any) *activities.InvokeToolResult {
	return &activities.InvokeToolResult{
		ToolCallID: call.ToolCallID,
		ToolName:   call.ToolName,
		Input:      call.Input,
		Output:     ai.ToolResultOutput{Type: "json", Value: value},
		Result:     value,
	}
}

func subagentToolError(call AgentToolCall, err error) error {
	return fmt.Errorf("%s: %w", call.ToolName, err)
}

func publishSubagentProgress(ctx workflow.Context, input AgentInput, snapshot SubagentSnapshot, writeRecords bool, activityOptions ...ActivityOptions) error {
	execution := input.SubagentExecution
	if execution == nil || execution.ParentWorkflowID == "" {
		return nil
	}
	snapshot.SubagentID = execution.SubagentID
	snapshot.ToolCallID = execution.ToolCallID
	snapshot.ToolName = execution.ToolName
	snapshot.UpdatedAt = workflow.Now(ctx)
	if streamID := updateStreamID(ctx, input); writeRecords && streamID != "" {
		data := map[string]any{
			"subagentId":   snapshot.SubagentID,
			"toolCallId":   snapshot.ToolCallID,
			"toolName":     snapshot.ToolName,
			"workflowId":   snapshot.WorkflowID,
			"runId":        snapshot.RunID,
			"sequence":     snapshot.Sequence,
			"stepNumber":   snapshot.StepNumber,
			"stepType":     snapshot.StepType,
			"text":         snapshot.Text,
			"toolCalls":    snapshot.ToolCalls,
			"finishReason": snapshot.FinishReason,
			"error":        snapshot.Error,
		}
		scope := input.Stream.Scope
		if scope.AgentID == "" {
			scope.AgentID = input.AgentID
		}
		if err := WriteRecord(ctx, streamID, updates.WorkflowRecord{
			RecordID:      "subagent:" + snapshot.SubagentID,
			RecordVersion: snapshot.Sequence,
			Kind:          updates.RecordKindSubagent,
			Status:        string(snapshot.Status),
			Data:          data,
			Scope:         scope,
		}, "", activityOptions...); err != nil {
			return err
		}
	}
	_ = workflow.SignalExternalWorkflow(ctx, execution.ParentWorkflowID, execution.ParentRunID, SubagentProgressSignalName, snapshot).Get(ctx, nil)
	return nil
}

func drainSubagentMessages(ctx workflow.Context, input AgentInput) []activities.Message {
	if input.SubagentExecution == nil {
		return nil
	}
	channel := workflow.GetSignalChannel(ctx, SubagentMessageSignalName)
	var messages []activities.Message
	for {
		var message SubagentMessage
		if !channel.ReceiveAsync(&message) {
			return messages
		}
		if strings.TrimSpace(message.Content) == "" {
			continue
		}
		messages = append(messages, activities.Message{Role: ai.RoleUser, Content: []activities.Part{{Type: "text", Text: message.Content}}})
	}
}
