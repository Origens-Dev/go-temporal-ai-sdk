package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

func normalizeSchema(schema any) any {
	switch value := schema.(type) {
	case nil:
		return map[string]any{"type": "object", "properties": map[string]any{}}
	case json.RawMessage:
		var out any
		if err := json.Unmarshal(value, &out); err == nil {
			return out
		}
		return value
	default:
		return value
	}
}

func ToolDefinitionFromAI(name string, tool ai.Tool) ToolDefinition {
	if tool.Name != "" {
		name = tool.Name
	}
	return ToolDefinition{
		Name:             name,
		Title:            tool.Title,
		Description:      tool.Description,
		InputSchema:      tool.InputSchema,
		OutputSchema:     tool.OutputSchema,
		InputExamples:    tool.InputExamples,
		Strict:           tool.Strict,
		ProviderOptions:  tool.ProviderOptions,
		ProviderMetadata: tool.ProviderMetadata,
		ToolMetadata:     tool.ToolMetadata,
		Type:             tool.Type,
		ID:               tool.ID,
		Args:             tool.Args,
		RequiresApproval: tool.RequiresApproval,
	}
}

func ToolDefinitionsFromAI(tools map[string]ai.Tool) []ToolDefinition {
	if len(tools) == 0 {
		return nil
	}
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	sort.Strings(names)
	definitions := make([]ToolDefinition, 0, len(tools))
	for _, name := range names {
		tool := tools[name]
		definitions = append(definitions, ToolDefinitionFromAI(name, tool))
	}
	return definitions
}

func (definition ToolDefinition) ToModelTool() ai.ModelTool {
	toolType := definition.Type
	if toolType == "" || toolType == "function" || toolType == "dynamic" {
		return ai.ModelTool{
			Type:            "function",
			Name:            definition.Name,
			Description:     definition.Description,
			InputSchema:     normalizeSchema(definition.InputSchema),
			InputExamples:   definition.InputExamples,
			Strict:          definition.Strict,
			ProviderOptions: definition.ProviderOptions,
			ToolMetadata:    definition.ToolMetadata,
		}
	}
	return ai.ModelTool{
		Type:         "provider",
		Name:         definition.Name,
		ID:           definition.ID,
		Args:         definition.Args,
		ToolMetadata: definition.ToolMetadata,
	}
}

func (definition ToolDefinition) ToAI() ai.Tool {
	return ai.Tool{
		Name:             definition.Name,
		Title:            definition.Title,
		Description:      definition.Description,
		InputSchema:      definition.InputSchema,
		OutputSchema:     definition.OutputSchema,
		InputExamples:    definition.InputExamples,
		Strict:           definition.Strict,
		ProviderOptions:  definition.ProviderOptions,
		ProviderMetadata: definition.ProviderMetadata,
		ToolMetadata:     definition.ToolMetadata,
		Type:             definition.Type,
		ID:               definition.ID,
		Args:             definition.Args,
		RequiresApproval: definition.RequiresApproval,
	}
}

func ModelToolsFromDefinitions(definitions []ToolDefinition, choice ai.ToolChoice) []ai.ModelTool {
	if len(definitions) == 0 || choice.Type == "none" {
		return nil
	}
	out := make([]ai.ModelTool, 0, len(definitions))
	for _, definition := range definitions {
		if definition.Name == "" {
			continue
		}
		if choice.Type == "tool" && choice.ToolName != "" && choice.ToolName != definition.Name {
			continue
		}
		out = append(out, definition.ToModelTool())
	}
	return out
}

func (a *Activities) InvokeTool(ctx context.Context, args InvokeToolArgs) (*InvokeToolResult, error) {
	if args.ToolCallID == "" {
		return nil, fmt.Errorf("toolCallId is required")
	}
	if args.ToolName == "" {
		return nil, fmt.Errorf("toolName is required")
	}
	runtime, err := a.resolveRuntime(ctx, runtimeScope(args.AgentID, args.CompiledRevision))
	if err != nil {
		return nil, err
	}
	tool, ok := runtime.tool(args.ToolName)
	dynamic := ok && tool.Type == "dynamic"
	metadata := ai.ProviderMetadata(nil)
	toolMetadata := args.ToolMetadata
	if ok {
		metadata = tool.ProviderMetadata
		toolMetadata = mergeProviderMetadata(tool.ToolMetadata, args.ToolMetadata)
	}
	if !ok {
		return finishToolResult(ctx, runtime, args, toolErrorResult(args, fmt.Errorf("tool %q is not registered", args.ToolName), nil, toolMetadata))
	}
	call := ai.ToolCall{
		ToolCallID:       args.ToolCallID,
		ToolName:         args.ToolName,
		Input:            args.Input,
		Dynamic:          dynamic,
		ToolMetadata:     toolMetadata,
		ProviderMetadata: metadata,
	}
	if err := ai.ValidateToolInput(tool, args.Input); err != nil {
		return finishToolResult(ctx, runtime, args, toolErrorResult(args, err, call.ProviderMetadata, call.ToolMetadata))
	}
	if args.Approval != nil {
		if args.Approval.Approved == nil || !*args.Approval.Approved {
			return finishToolResult(ctx, runtime, args, deniedToolResult(args, call, args.Approval.Reason))
		}
		if tool.NeedsApproval != nil {
			decision, err := ai.ResolveToolApproval(ctx, map[string]ai.Tool{args.ToolName: tool}, call)
			if err != nil {
				return finishToolResult(ctx, runtime, args, toolErrorResult(args, err, call.ProviderMetadata, call.ToolMetadata))
			}
			if decision.Type == ai.ApprovalDecisionDenied {
				return finishToolResult(ctx, runtime, args, deniedToolResult(args, call, decision.Reason))
			}
		}
	} else if tool.RequiresApproval || tool.NeedsApproval != nil {
		decision, err := ai.ResolveToolApproval(ctx, map[string]ai.Tool{args.ToolName: tool}, call)
		if err != nil {
			return finishToolResult(ctx, runtime, args, toolErrorResult(args, err, call.ProviderMetadata, call.ToolMetadata))
		}
		if ai.ApprovalBlocksToolExecution(decision) {
			return finishToolResult(ctx, runtime, args, deniedToolResult(args, call, decision.Reason))
		}
	}
	if tool.Execute == nil {
		return finishToolResult(ctx, runtime, args, toolErrorResult(args, fmt.Errorf("tool %q has no execute function", args.ToolName), call.ProviderMetadata, call.ToolMetadata))
	}
	output, err := tool.Execute(ctx, call, ai.ToolExecutionOptions{
		ToolCallID: args.ToolCallID,
		Messages:   MessagesToAI(args.Messages),
		Context:    contextWithScope(args.Context, args.Scope),
		Sandbox:    runtime.Sandbox,
	})
	isError := err != nil
	modelOutputInput := output
	if isError {
		modelOutputInput = err.Error()
	} else if err := ai.ValidateToolOutput(tool, output); err != nil {
		isError = true
		modelOutputInput = err.Error()
	}
	modelOutput, modelErr := ai.CreateToolModelOutput(tool, args.ToolCallID, args.Input, modelOutputInput, isError)
	if modelErr != nil {
		isError = true
		modelOutput = ai.ToolResultOutput{Type: "error-text", Value: modelErr.Error()}
	}
	return finishToolResult(ctx, runtime, args, &InvokeToolResult{
		ToolCallID:       args.ToolCallID,
		ToolName:         args.ToolName,
		Input:            args.Input,
		Output:           modelOutput,
		Result:           output,
		IsError:          isError,
		Dynamic:          call.Dynamic,
		ToolMetadata:     call.ToolMetadata,
		ProviderMetadata: call.ProviderMetadata,
	})
}

func contextWithScope(contextValue any, scope updates.Scope) any {
	if scope.DisplayMode == "" &&
		scope.AgentID == "" &&
		scope.TaskID == "" &&
		scope.TaskTitle == "" &&
		scope.SkillName == "" &&
		scope.StepID == "" &&
		scope.StepNumber == nil &&
		scope.StepType == "" {
		return contextValue
	}

	merged := map[string]any{}
	switch value := contextValue.(type) {
	case nil:
	case map[string]any:
		for key, inner := range value {
			merged[key] = inner
		}
	case map[string]string:
		for key, inner := range value {
			merged[key] = inner
		}
	default:
		bytes, err := json.Marshal(value)
		if err != nil {
			return contextValue
		}
		_ = json.Unmarshal(bytes, &merged)
	}

	if scope.DisplayMode != "" {
		merged["displayMode"] = string(scope.DisplayMode)
	}
	if scope.AgentID != "" {
		merged["agentId"] = scope.AgentID
	}
	if scope.TaskID != "" {
		merged["taskId"] = scope.TaskID
	}
	if scope.TaskTitle != "" {
		merged["taskTitle"] = scope.TaskTitle
	}
	if scope.SkillName != "" {
		merged["skillName"] = scope.SkillName
	}
	if scope.StepID != "" {
		merged["stepId"] = scope.StepID
	}
	if scope.StepNumber != nil {
		merged["stepNumber"] = *scope.StepNumber
	}
	if scope.StepType != "" {
		merged["stepType"] = scope.StepType
	}
	return merged
}

func deniedToolResult(args InvokeToolArgs, call ai.ToolCall, reason string) *InvokeToolResult {
	return &InvokeToolResult{
		ToolCallID:       args.ToolCallID,
		ToolName:         args.ToolName,
		Input:            args.Input,
		Output:           ai.ToolResultOutput{Type: "execution-denied", Reason: reason},
		Dynamic:          call.Dynamic,
		ToolMetadata:     call.ToolMetadata,
		ProviderMetadata: call.ProviderMetadata,
	}
}

func toolErrorResult(args InvokeToolArgs, err error, metadata ai.ProviderMetadata, toolMetadata ai.ProviderMetadata) *InvokeToolResult {
	text := "tool execution failed"
	if err != nil {
		text = err.Error()
	}
	return &InvokeToolResult{
		ToolCallID:       args.ToolCallID,
		ToolName:         args.ToolName,
		Input:            args.Input,
		Output:           ai.ToolResultOutput{Type: "error-text", Value: text},
		IsError:          true,
		ToolMetadata:     toolMetadata,
		ProviderMetadata: metadata,
	}
}

func mergeProviderMetadata(base ai.ProviderMetadata, override ai.ProviderMetadata) ai.ProviderMetadata {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := ai.ProviderMetadata{}
	for key, value := range base {
		out[key] = value
	}
	for key, value := range override {
		out[key] = value
	}
	return out
}

func (r AgentRuntime) tool(name string) (ai.Tool, bool) {
	if len(r.Tools) == 0 {
		return ai.Tool{}, false
	}
	tool, ok := r.Tools[name]
	return tool, ok
}

func finishToolResult(ctx context.Context, runtime AgentRuntime, args InvokeToolArgs, result *InvokeToolResult) (*InvokeToolResult, error) {
	result, err := compactToolArtifacts(ctx, runtime.ArtifactStore, args, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
