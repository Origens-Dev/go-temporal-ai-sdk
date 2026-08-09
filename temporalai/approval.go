package temporalai

import (
	"fmt"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ToolApprovalSignalName = "go-temporal-ai-sdk.tool-approval-response"
)

type AgentToolApprovalOptions struct {
	SignalName string         `json:"signalName,omitempty"`
	Timeout    time.Duration  `json:"timeout,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

type ToolApprovalRequest struct {
	StreamID     string              `json:"streamId,omitempty"`
	ApprovalID   string              `json:"approvalId"`
	ToolCallID   string              `json:"toolCallId"`
	ToolName     string              `json:"toolName"`
	Input        any                 `json:"input,omitempty"`
	ToolMetadata ai.ProviderMetadata `json:"toolMetadata,omitempty"`
	Metadata     map[string]any      `json:"metadata,omitempty"`
	Timeout      time.Duration       `json:"timeout,omitempty"`
	SignalName   string              `json:"signalName,omitempty"`
	updates.Scope
}

type ToolApprovalResponse struct {
	ApprovalID string `json:"approvalId"`
	ToolCallID string `json:"toolCallId,omitempty"`
	Approved   bool   `json:"approved"`
	Reason     string `json:"reason,omitempty"`
	TimedOut   bool   `json:"timedOut,omitempty"`
	Canceled   bool   `json:"canceled,omitempty"`
}

func RequestToolApproval(ctx workflow.Context, request ToolApprovalRequest, activityOptions ...ActivityOptions) (*ToolApprovalResponse, error) {
	return requestToolApproval(ctx, request, durableRecordsEnabled(ctx), activityOptions...)
}

func requestToolApproval(ctx workflow.Context, request ToolApprovalRequest, writeRecords bool, activityOptions ...ActivityOptions) (*ToolApprovalResponse, error) {
	if request.ApprovalID == "" {
		return nil, fmt.Errorf("approvalId is required")
	}
	if request.ToolCallID == "" {
		return nil, fmt.Errorf("toolCallId is required")
	}
	if request.ToolName == "" {
		return nil, fmt.Errorf("toolName is required")
	}
	if writeRecords && request.StreamID != "" {
		if err := WriteRecord(ctx, request.StreamID, toolApprovalRecord(request, nil, 1), "", activityOptions...); err != nil {
			return nil, err
		}
	}
	response, waitErr := waitForToolApprovalResponse(ctx, request)
	if writeRecords && request.StreamID != "" {
		recordCtx := ctx
		if waitErr != nil && temporal.IsCanceledError(waitErr) {
			recordCtx, _ = workflow.NewDisconnectedContext(ctx)
		}
		if err := WriteRecord(recordCtx, request.StreamID, toolApprovalRecord(request, &response, 2), "", activityOptions...); err != nil {
			return nil, err
		}
	}
	if waitErr != nil {
		return nil, waitErr
	}
	return &response, nil
}

func waitForToolApprovalResponse(ctx workflow.Context, request ToolApprovalRequest) (ToolApprovalResponse, error) {
	signalName := request.SignalName
	if signalName == "" {
		signalName = ToolApprovalResponseSignalName(request.ApprovalID)
	}
	signalCh := workflow.GetSignalChannel(ctx, signalName)
	var response ToolApprovalResponse
	for {
		selector := workflow.NewSelector(ctx)
		received := false
		selector.AddReceive(signalCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &response)
			received = true
		})
		canceled := false
		selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {
			canceled = true
		})
		timedOut := false
		if request.Timeout > 0 {
			timer := workflow.NewTimer(ctx, request.Timeout)
			selector.AddFuture(timer, func(workflow.Future) {
				timedOut = true
			})
		}
		selector.Select(ctx)
		if canceled {
			return ToolApprovalResponse{
				ApprovalID: request.ApprovalID,
				ToolCallID: request.ToolCallID,
				Approved:   false,
				Reason:     "approval canceled",
				Canceled:   true,
			}, temporal.NewCanceledError("approval canceled")
		}
		if timedOut {
			return ToolApprovalResponse{
				ApprovalID: request.ApprovalID,
				ToolCallID: request.ToolCallID,
				Approved:   false,
				Reason:     "approval timed out",
				TimedOut:   true,
			}, nil
		}
		if !received || response.ApprovalID != request.ApprovalID {
			continue
		}
		if response.ToolCallID == "" {
			response.ToolCallID = request.ToolCallID
		}
		return response, nil
	}
}

func ToolApprovalResponseSignalName(approvalID string) string {
	if approvalID == "" {
		return ToolApprovalSignalName
	}
	return fmt.Sprintf("%s.%s", ToolApprovalSignalName, approvalID)
}

func toolApprovalState(response *ToolApprovalResponse) *activities.ToolApprovalState {
	if response == nil {
		return nil
	}
	return &activities.ToolApprovalState{
		ApprovalID: response.ApprovalID,
		Approved:   &response.Approved,
		Reason:     response.Reason,
	}
}

func toolApprovalRecord(request ToolApprovalRequest, response *ToolApprovalResponse, version int) updates.WorkflowRecord {
	status := "pending"
	data := map[string]any{
		"interactionId":   request.ApprovalID,
		"interactionType": "tool-approval",
		"title":           "Review " + request.ToolName,
		"questions": []any{map[string]any{
			"id":     request.ApprovalID,
			"prompt": "Allow this tool call?",
			"choices": []any{
				map[string]any{"id": "approve", "label": "Approve", "value": map[string]any{"approved": true}},
				map[string]any{"id": "deny", "label": "Deny", "value": map[string]any{"approved": false}},
			},
			"required": true,
		}},
		"origin": map[string]any{
			"toolCallId": request.ToolCallID,
			"toolName":   request.ToolName,
			"input":      request.Input,
		},
	}
	if len(request.ToolMetadata) > 0 {
		data["toolMetadata"] = request.ToolMetadata
	}
	if len(request.Metadata) > 0 {
		data["metadata"] = request.Metadata
	}
	if response != nil {
		status = "denied"
		if response.Approved {
			status = "approved"
		} else if response.TimedOut {
			status = "timed-out"
		} else if response.Canceled {
			status = "canceled"
		}
		data["answer"] = map[string]any{
			"approved": response.Approved,
			"reason":   response.Reason,
			"timedOut": response.TimedOut,
			"canceled": response.Canceled,
		}
	}
	return updates.WorkflowRecord{
		RecordID:      "interaction:" + request.ApprovalID,
		RecordVersion: version,
		Kind:          updates.RecordKindInteraction,
		Status:        status,
		Data:          data,
		Scope:         request.Scope,
	}
}
