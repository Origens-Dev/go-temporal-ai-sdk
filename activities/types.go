package activities

import (
	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

const (
	InvokeModelActivity          = "go-temporal-ai-sdk.InvokeModel"
	GenerateObjectActivity       = "go-temporal-ai-sdk.GenerateObject"
	StreamObjectActivity         = "go-temporal-ai-sdk.StreamObject"
	InvokeModelStreamActivity    = "go-temporal-ai-sdk.InvokeModelStream"
	InvokeEmbeddingModelActivity = "go-temporal-ai-sdk.InvokeEmbeddingModel"
	InvokeToolActivity           = "go-temporal-ai-sdk.InvokeTool"
	WriteRecordActivity          = "go-temporal-ai-sdk.WriteRecord"
	EndStreamActivity            = "go-temporal-ai-sdk.EndStream"
)

type ToolExecutionBoundary string

const (
	ToolExecutionBoundaryAuto          ToolExecutionBoundary = "auto"
	ToolExecutionBoundaryActivity      ToolExecutionBoundary = "activity"
	ToolExecutionBoundaryLocalActivity ToolExecutionBoundary = "local-activity"
)

type InvokeModelArgs struct {
	AgentID          string                   `json:"agentId,omitempty"`
	CompiledRevision string                   `json:"compiledRevision,omitempty"`
	ModelID          string                   `json:"modelId"`
	Options          LanguageModelCallOptions `json:"options"`
}

type InvokeModelResult = LanguageModelGenerateResult

type GenerateObjectArgs struct {
	AgentID          string                `json:"agentId,omitempty"`
	CompiledRevision string                `json:"compiledRevision,omitempty"`
	ModelID          string                `json:"modelId"`
	Options          GenerateObjectOptions `json:"options"`
}

type StreamObjectArgs struct {
	AgentID          string              `json:"agentId,omitempty"`
	CompiledRevision string              `json:"compiledRevision,omitempty"`
	ModelID          string              `json:"modelId"`
	Options          StreamObjectOptions `json:"options"`
}

type StreamObjectResult struct {
	StreamParts     []ObjectStreamPart       `json:"streamParts,omitempty"`
	Elements        []any                    `json:"elements,omitempty"`
	Request         *ai.RequestMetadata      `json:"request,omitempty"`
	Response        *ResponseMetadata        `json:"response,omitempty"`
	PreviewReceipts []updates.PreviewReceipt `json:"previewReceipts,omitempty"`
}

type InvokeModelStreamArgs struct {
	AgentID          string                   `json:"agentId,omitempty"`
	CompiledRevision string                   `json:"compiledRevision,omitempty"`
	ModelID          string                   `json:"modelId"`
	Options          LanguageModelCallOptions `json:"options"`
}

type InvokeModelStreamResult struct {
	Result          *LanguageModelGenerateResult `json:"result,omitempty"`
	StreamParts     []StreamPart                 `json:"streamParts,omitempty"`
	Request         *ai.RequestMetadata          `json:"request,omitempty"`
	Response        *ResponseMetadata            `json:"response,omitempty"`
	PreviewReceipts []updates.PreviewReceipt     `json:"previewReceipts,omitempty"`
}

type InvokeEmbeddingModelArgs struct {
	AgentID          string             `json:"agentId,omitempty"`
	CompiledRevision string             `json:"compiledRevision,omitempty"`
	ModelID          string             `json:"modelId"`
	Values           []string           `json:"values"`
	ProviderOptions  ai.ProviderOptions `json:"providerOptions,omitempty"`
	Headers          map[string]string  `json:"headers,omitempty"`
}

type InvokeEmbeddingModelResult = ai.EmbeddingModelResult

type ToolDefinition struct {
	Name              string                `json:"name"`
	Title             string                `json:"title,omitempty"`
	Description       string                `json:"description,omitempty"`
	InputSchema       any                   `json:"inputSchema,omitempty"`
	OutputSchema      any                   `json:"outputSchema,omitempty"`
	InputExamples     []any                 `json:"inputExamples,omitempty"`
	Strict            *bool                 `json:"strict,omitempty"`
	ProviderOptions   ai.ProviderOptions    `json:"providerOptions,omitempty"`
	ProviderMetadata  ai.ProviderMetadata   `json:"providerMetadata,omitempty"`
	ToolMetadata      ai.ProviderMetadata   `json:"toolMetadata,omitempty"`
	Type              string                `json:"type,omitempty"`
	ID                string                `json:"id,omitempty"`
	Args              any                   `json:"args,omitempty"`
	RequiresApproval  bool                  `json:"requiresApproval,omitempty"`
	ExecutionBoundary ToolExecutionBoundary `json:"executionBoundary,omitempty"`
}

type InvokeToolArgs struct {
	AgentID          string              `json:"agentId,omitempty"`
	CompiledRevision string              `json:"compiledRevision,omitempty"`
	ToolCallID       string              `json:"toolCallId"`
	ToolName         string              `json:"toolName"`
	Input            any                 `json:"input,omitempty"`
	Messages         []Message           `json:"messages,omitempty"`
	Context          any                 `json:"context,omitempty"`
	ToolMetadata     ai.ProviderMetadata `json:"toolMetadata,omitempty"`
	Scope            updates.Scope       `json:"scope,omitempty"`
	Artifacts        *ToolArtifactPolicy `json:"artifacts,omitempty"`
	Approval         *ToolApprovalState  `json:"approval,omitempty"`
}

type ToolApprovalState struct {
	ApprovalID string `json:"approvalId,omitempty"`
	Approved   *bool  `json:"approved,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type InvokeToolResult struct {
	ToolCallID       string              `json:"toolCallId"`
	ToolName         string              `json:"toolName"`
	Input            any                 `json:"input,omitempty"`
	Output           ai.ToolResultOutput `json:"output"`
	Result           any                 `json:"result,omitempty"`
	IsError          bool                `json:"isError,omitempty"`
	Dynamic          bool                `json:"dynamic,omitempty"`
	ProviderExecuted bool                `json:"providerExecuted,omitempty"`
	Preliminary      bool                `json:"preliminary,omitempty"`
	ToolMetadata     ai.ProviderMetadata `json:"toolMetadata,omitempty"`
	ProviderMetadata ai.ProviderMetadata `json:"providerMetadata,omitempty"`
}

type WriteRecordArgs struct {
	Event updates.RecordUpsertEvent `json:"event"`
}

type EndStreamArgs struct {
	Event updates.StreamEndEvent `json:"event"`
}
