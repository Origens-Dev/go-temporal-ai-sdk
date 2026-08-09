// Package updates implements the Temporal durable updates protocol v2.
//
// Provider output is represented by provisional preview events. Only a
// workflow-authored RecordUpsertEvent makes output canonical and durable.
package updates

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const ProtocolVersion = 2

type EventType string

const (
	EventTypePreviewBegin    EventType = "preview-begin"
	EventTypePreviewChunk    EventType = "preview-chunk"
	EventTypePreviewSnapshot EventType = "preview-snapshot"
	EventTypePreviewEnd      EventType = "preview-end"
	EventTypeRecordUpsert    EventType = "record-upsert"
	EventTypeStreamEnd       EventType = "stream-end"
)

type Lane string

const (
	LaneText      Lane = "text"
	LaneReasoning Lane = "reasoning"
	LaneObject    Lane = "object"
	LaneToolInput Lane = "tool-input"
)

type DisplayMode string

const (
	DisplayModeAssistant DisplayMode = "assistant"
	DisplayModeTask      DisplayMode = "task"
	DisplayModeHidden    DisplayMode = "hidden"
)

type PreviewOutcome string

const (
	PreviewOutcomeSucceeded PreviewOutcome = "succeeded"
	PreviewOutcomeFailed    PreviewOutcome = "failed"
	PreviewOutcomeCanceled  PreviewOutcome = "canceled"
)

type PreviewStatus string

const (
	PreviewStatusActive    PreviewStatus = "active"
	PreviewStatusSucceeded PreviewStatus = "succeeded"
)

type RecordKind string

const (
	RecordKindMessage     RecordKind = "message"
	RecordKindTool        RecordKind = "tool"
	RecordKindInteraction RecordKind = "interaction"
	RecordKindTask        RecordKind = "task"
	RecordKindSubagent    RecordKind = "subagent"
)

type StreamOutcome string

const (
	StreamOutcomeCompleted StreamOutcome = "completed"
	StreamOutcomeFailed    StreamOutcome = "failed"
	StreamOutcomeCanceled  StreamOutcome = "canceled"
)

type Scope struct {
	DisplayMode DisplayMode `json:"displayMode,omitempty"`
	AgentID     string      `json:"agentId,omitempty"`
	TaskID      string      `json:"taskId,omitempty"`
	TaskTitle   string      `json:"taskTitle,omitempty"`
	SkillName   string      `json:"skillName,omitempty"`
	StepID      string      `json:"stepId,omitempty"`
	StepNumber  *int        `json:"stepNumber,omitempty"`
	StepType    string      `json:"stepType,omitempty"`
}

type Snapshot struct {
	Text     string `json:"text,omitempty"`
	Object   any    `json:"object,omitempty"`
	Elements []any  `json:"elements,omitempty"`
}

type BaseEvent struct {
	ProtocolVersion int       `json:"protocolVersion"`
	Type            EventType `json:"type"`
	EventID         string    `json:"eventId"`
	Cursor          string    `json:"cursor,omitempty"`
	StreamID        string    `json:"streamId"`
	// ConversationID is the durable session identity shared by one or more
	// execution streams. It remains optional for generic SDK consumers.
	ConversationID string `json:"conversationId,omitempty"`
	// AgentID is the compiler-owned agent definition that produced this event.
	// Connectors that cross a platform boundary can validate it against the
	// deployment manifest bound to the worker slot.
	AgentID    string `json:"agentId,omitempty"`
	OccurredAt int64  `json:"occurredAt"`
}

type PreviewRef struct {
	AttemptID      string `json:"attemptId"`
	TargetRecordID string `json:"targetRecordId"`
	Lane           Lane   `json:"lane"`
	Sequence       int    `json:"sequence"`
	Scope          Scope  `json:"scope,omitempty"`
}

type UpdateEvent interface {
	EventBase() BaseEvent
}

type PreviewBeginEvent struct {
	BaseEvent
	PreviewRef
}

func (e PreviewBeginEvent) EventBase() BaseEvent { return e.BaseEvent }

type PreviewChunkEvent struct {
	BaseEvent
	PreviewRef
	Chunk map[string]any `json:"chunk"`
}

func (e PreviewChunkEvent) EventBase() BaseEvent { return e.BaseEvent }

type PreviewSnapshotEvent struct {
	BaseEvent
	PreviewRef
	Snapshot Snapshot `json:"snapshot"`
}

func (e PreviewSnapshotEvent) EventBase() BaseEvent { return e.BaseEvent }

type PreviewEndEvent struct {
	BaseEvent
	PreviewRef
	Outcome  PreviewOutcome `json:"outcome"`
	Reason   string         `json:"reason,omitempty"`
	Snapshot *Snapshot      `json:"snapshot,omitempty"`
}

func (e PreviewEndEvent) EventBase() BaseEvent { return e.BaseEvent }

type WorkflowRecord struct {
	RecordID      string         `json:"recordId"`
	RecordVersion int            `json:"recordVersion"`
	Kind          RecordKind     `json:"kind"`
	Status        string         `json:"status"`
	Data          map[string]any `json:"data"`
	Scope         Scope          `json:"scope,omitempty"`
	UpdatedAt     int64          `json:"updatedAt"`
}

type RecordUpsertEvent struct {
	BaseEvent
	AcceptedAttemptID string         `json:"acceptedAttemptId,omitempty"`
	Record            WorkflowRecord `json:"record"`
}

func (e RecordUpsertEvent) EventBase() BaseEvent { return e.BaseEvent }

type StreamEndEvent struct {
	BaseEvent
	Outcome StreamOutcome `json:"outcome"`
	Error   string        `json:"error,omitempty"`
}

func (e StreamEndEvent) EventBase() BaseEvent { return e.BaseEvent }

type PreviewManifest struct {
	AttemptID      string        `json:"attemptId"`
	TargetRecordID string        `json:"targetRecordId"`
	Lane           Lane          `json:"lane"`
	Status         PreviewStatus `json:"status"`
	Sequence       int           `json:"sequence"`
	Snapshot       Snapshot      `json:"snapshot,omitempty"`
	Scope          Scope         `json:"scope,omitempty"`
	UpdatedAt      int64         `json:"updatedAt"`
}

type ReplayResponse struct {
	ProtocolVersion int               `json:"protocolVersion"`
	StreamID        string            `json:"streamId"`
	Cursor          string            `json:"cursor"`
	Events          []json.RawMessage `json:"events"`
	Previews        []PreviewManifest `json:"previews"`
	Records         []WorkflowRecord  `json:"records"`
	Terminal        *StreamEndEvent   `json:"terminal,omitempty"`
}

type PreviewReceipt struct {
	AttemptID      string         `json:"attemptId"`
	TargetRecordID string         `json:"targetRecordId"`
	Lane           Lane           `json:"lane"`
	Sequence       int            `json:"sequence"`
	Outcome        PreviewOutcome `json:"outcome"`
	Snapshot       Snapshot       `json:"snapshot,omitempty"`
	Scope          Scope          `json:"scope,omitempty"`
}

var (
	ErrInvalidEvent   = errors.New("invalid update event")
	ErrRecordConflict = errors.New("record version conflict")
	ErrEventConflict  = errors.New("event identity conflict")
)

func DecodeEvent(data []byte) (UpdateEvent, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if raw, ok := envelope["cursor"]; ok {
		var cursor string
		if err := json.Unmarshal(raw, &cursor); err != nil || cursor == "" {
			return nil, fmt.Errorf("%w: cursor must be a non-empty string when present", ErrInvalidEvent)
		}
	}
	if raw, ok := envelope["acceptedAttemptId"]; ok {
		var attemptID string
		if err := json.Unmarshal(raw, &attemptID); err != nil || attemptID == "" {
			return nil, fmt.Errorf("%w: acceptedAttemptId must be a non-empty string when present", ErrInvalidEvent)
		}
	}
	var header BaseEvent
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	var event UpdateEvent
	switch header.Type {
	case EventTypePreviewBegin:
		event = &PreviewBeginEvent{}
	case EventTypePreviewChunk:
		event = &PreviewChunkEvent{}
	case EventTypePreviewSnapshot:
		event = &PreviewSnapshotEvent{}
	case EventTypePreviewEnd:
		event = &PreviewEndEvent{}
	case EventTypeRecordUpsert:
		event = &RecordUpsertEvent{}
	case EventTypeStreamEnd:
		event = &StreamEndEvent{}
	default:
		return nil, fmt.Errorf("%w: unknown type %q", ErrInvalidEvent, header.Type)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(event); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if err := ValidateEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func ValidateEvent(event UpdateEvent) error {
	if event == nil {
		return fmt.Errorf("%w: event is nil", ErrInvalidEvent)
	}
	base := event.EventBase()
	if base.ProtocolVersion != ProtocolVersion || base.EventID == "" || base.StreamID == "" || base.OccurredAt < 0 {
		return fmt.Errorf("%w: invalid base fields", ErrInvalidEvent)
	}
	switch value := event.(type) {
	case *PreviewBeginEvent:
		if base.Type != EventTypePreviewBegin {
			return invalidConcreteType(base.Type, EventTypePreviewBegin)
		}
		return validatePreview(value.PreviewRef)
	case PreviewBeginEvent:
		if base.Type != EventTypePreviewBegin {
			return invalidConcreteType(base.Type, EventTypePreviewBegin)
		}
		return validatePreview(value.PreviewRef)
	case *PreviewChunkEvent:
		if base.Type != EventTypePreviewChunk {
			return invalidConcreteType(base.Type, EventTypePreviewChunk)
		}
		return validatePreviewChunk(value.PreviewRef, value.Chunk)
	case PreviewChunkEvent:
		if base.Type != EventTypePreviewChunk {
			return invalidConcreteType(base.Type, EventTypePreviewChunk)
		}
		return validatePreviewChunk(value.PreviewRef, value.Chunk)
	case *PreviewSnapshotEvent:
		if base.Type != EventTypePreviewSnapshot {
			return invalidConcreteType(base.Type, EventTypePreviewSnapshot)
		}
		return validatePreview(value.PreviewRef)
	case PreviewSnapshotEvent:
		if base.Type != EventTypePreviewSnapshot {
			return invalidConcreteType(base.Type, EventTypePreviewSnapshot)
		}
		return validatePreview(value.PreviewRef)
	case *PreviewEndEvent:
		if base.Type != EventTypePreviewEnd {
			return invalidConcreteType(base.Type, EventTypePreviewEnd)
		}
		return validatePreviewEnd(value.PreviewRef, value.Outcome)
	case PreviewEndEvent:
		if base.Type != EventTypePreviewEnd {
			return invalidConcreteType(base.Type, EventTypePreviewEnd)
		}
		return validatePreviewEnd(value.PreviewRef, value.Outcome)
	case *RecordUpsertEvent:
		if base.Type != EventTypeRecordUpsert {
			return invalidConcreteType(base.Type, EventTypeRecordUpsert)
		}
		return validateRecord(value.Record)
	case RecordUpsertEvent:
		if base.Type != EventTypeRecordUpsert {
			return invalidConcreteType(base.Type, EventTypeRecordUpsert)
		}
		return validateRecord(value.Record)
	case *StreamEndEvent:
		if base.Type != EventTypeStreamEnd {
			return invalidConcreteType(base.Type, EventTypeStreamEnd)
		}
		return validateStreamOutcome(value.Outcome)
	case StreamEndEvent:
		if base.Type != EventTypeStreamEnd {
			return invalidConcreteType(base.Type, EventTypeStreamEnd)
		}
		return validateStreamOutcome(value.Outcome)
	default:
		return fmt.Errorf("%w: unsupported event %T", ErrInvalidEvent, event)
	}
}

func invalidConcreteType(got, want EventType) error {
	return fmt.Errorf("%w: event type %q does not match concrete event %q", ErrInvalidEvent, got, want)
}

func validatePreviewChunk(ref PreviewRef, chunk map[string]any) error {
	if err := validatePreview(ref); err != nil {
		return err
	}
	if kind, _ := chunk["type"].(string); kind == "" {
		return fmt.Errorf("%w: chunk.type is required", ErrInvalidEvent)
	}
	return nil
}

func validatePreviewEnd(ref PreviewRef, outcome PreviewOutcome) error {
	if err := validatePreview(ref); err != nil {
		return err
	}
	if outcome != PreviewOutcomeSucceeded && outcome != PreviewOutcomeFailed && outcome != PreviewOutcomeCanceled {
		return fmt.Errorf("%w: invalid preview outcome", ErrInvalidEvent)
	}
	return nil
}

func validateRecord(record WorkflowRecord) error {
	if record.RecordID == "" || record.RecordVersion < 1 || record.Kind == "" || record.Status == "" || record.Data == nil || record.UpdatedAt < 0 {
		return fmt.Errorf("%w: invalid record", ErrInvalidEvent)
	}
	switch record.Kind {
	case RecordKindMessage, RecordKindTool, RecordKindInteraction, RecordKindTask, RecordKindSubagent:
		return validateScope(record.Scope)
	default:
		return fmt.Errorf("%w: invalid record kind %q", ErrInvalidEvent, record.Kind)
	}
}

func validateStreamOutcome(outcome StreamOutcome) error {
	if outcome != StreamOutcomeCompleted && outcome != StreamOutcomeFailed && outcome != StreamOutcomeCanceled {
		return fmt.Errorf("%w: invalid stream outcome", ErrInvalidEvent)
	}
	return nil
}

func validatePreview(ref PreviewRef) error {
	if ref.AttemptID == "" || ref.TargetRecordID == "" || ref.Sequence < 0 {
		return fmt.Errorf("%w: invalid preview identity", ErrInvalidEvent)
	}
	switch ref.Lane {
	case LaneText, LaneReasoning, LaneObject, LaneToolInput:
		return validateScope(ref.Scope)
	default:
		return fmt.Errorf("%w: invalid preview lane %q", ErrInvalidEvent, ref.Lane)
	}
}

func validateScope(scope Scope) error {
	if scope.StepNumber != nil && *scope.StepNumber < 0 {
		return fmt.Errorf("%w: scope.stepNumber must be non-negative", ErrInvalidEvent)
	}
	return nil
}
